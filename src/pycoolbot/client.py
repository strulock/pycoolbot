"""Async WebSocket client for the CoolBot Pro cloud service."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import aiohttp

from .const import (
    CMD_APP_SYNC,
    CMD_DASH_GZIPPED,
    CMD_HARDWARE,
    CMD_LOAD_PROFILE_GZIPPED,
    CMD_LOGIN,
    CMD_PING,
    CMD_RESPONSE,
    COMPRESSED_CMDS,
    LIVE_PINS,
    PIN_MAC_ADDRESS,
    PUSH_INTERVAL_SECONDS,
    STATUS_OK,
    WS_URL,
)
from .models import CoolbotDevice, build_devices, merge_dashboards, target_for
from .protocol import (
    MAX_MSG_ID,
    Frame,
    PinUpdate,
    decode_frames,
    encode_frame,
    inflate_json,
    login_body,
    parse_pin_update,
    status_message,
)

_LOGGER = logging.getLogger(__name__)


class CoolbotError(Exception):
    """Base error."""


class CoolbotAuthError(CoolbotError):
    """Credentials were rejected."""


class CoolbotConnectionError(CoolbotError):
    """The socket could not be established or was lost."""


def _discard(future: asyncio.Future[int]) -> None:
    """Retire a pending response, leaving nothing for asyncio to report.

    A connection closing while a send is suspended lets the reader fail this
    future before the writer raises its own error. Nobody awaits it after that,
    so its exception has to be retrieved here; otherwise asyncio reports it as
    never retrieved when the future is collected. The reader also clears the
    pending map, so the future is taken directly rather than looked up.
    """
    if future.cancelled():
        return
    if future.done():
        future.exception()
    else:
        future.cancel()


class CoolbotClient:
    """Speaks the Blynk app protocol to the CoolBot cloud.

    Usage::

        async with CoolbotClient(email, password) as client:
            devices = await client.async_get_devices()

    The client keeps every pin value it has seen, so a long-lived instance serves
    push updates as they arrive. ``async_get_devices`` waits for a live push
    before returning, which is what makes a reading trustworthy: the server
    replays a cached snapshot on connect, so whatever arrives first can be
    minutes out of date.
    """

    def __init__(
        self,
        email: str,
        password: str,
        session: aiohttp.ClientSession | None = None,
        *,
        request_timeout: float = 20.0,
    ) -> None:
        self._email = email
        self._password = password
        self._session = session
        self._owns_session = session is None
        self._request_timeout = request_timeout

        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._reader: asyncio.Task[None] | None = None
        # Wraps rather than counting on: the id is 16 bits on the wire, and a
        # socket kept alive for weeks sends more frames than that.
        self._ids = itertools.cycle(range(1, MAX_MSG_ID + 1))

        self._pins: dict[str, dict[str, str]] = {}
        self._live_at: dict[str, datetime] = {}
        self._profile_records: list[dict[str, Any]] = []

        self._responses: dict[int, asyncio.Future[int]] = {}
        self._profile_ready = asyncio.Event()
        self._snapshot_ready = asyncio.Event()
        #: Slots a subscription is currently waiting to hear pins for.
        self._replay_expected: set[str] = set()
        self._replay_seen: set[str] = set()
        self._replay_done = asyncio.Event()
        #: Slots whose identity a subscription has yet to confirm, and the pins
        #: that arrived for them meanwhile, with the command and arrival time.
        self._awaiting_identity: set[str] = set()
        self._held: dict[str, list[tuple[int, PinUpdate, datetime]]] = {}
        self._live_push = asyncio.Event()
        self._listeners: list[Callable[[], None]] = []
        self._closed = False

    # --- lifecycle ----------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self.async_connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.async_close()

    async def async_connect(self) -> None:
        """Open the socket, authenticate, and request the account profile."""
        if self._session is None:
            self._session = aiohttp.ClientSession()

        try:
            self._ws = await self._session.ws_connect(WS_URL, heartbeat=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise CoolbotConnectionError(f"could not connect: {err}") from err

        self._reader = asyncio.create_task(self._read_loop())

        status = await self._request(CMD_LOGIN, login_body(self._email, self._password))
        if status != STATUS_OK:
            # 5 = user not registered, 6 = not allowed; both mean bad credentials.
            raise CoolbotAuthError(f"login rejected: {status_message(status)}")
        # Deliberately without the address: consumers expose this logger to
        # their users, who share debug logs when asking for help.
        _LOGGER.debug("logged in")

        await self._load_profile()
        await self._sync_dashboards()

    async def async_close(self) -> None:
        """Close the socket and release resources.

        Never raises: closing is best-effort cleanup, usually done while
        handling some other failure, and an error here must not replace it.
        """
        self._closed = True
        if self._reader:
            self._reader.cancel()
            try:
                await self._reader
            except asyncio.CancelledError:
                pass
            self._reader = None
        if self._ws is not None and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001 - best-effort close, see docstring
                _LOGGER.debug("error while closing the websocket", exc_info=True)
        self._ws = None
        if self._owns_session and self._session is not None:
            try:
                await self._session.close()
            except Exception:  # noqa: BLE001 - best-effort close, see docstring
                _LOGGER.debug("error while closing the session", exc_info=True)
            self._session = None

    # --- public API ---------------------------------------------------------

    def add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Register a callback for live device updates, returning its remover.

        The callback fires when a device pushes something new: a reading whose
        arrival moves the device's freshness on, or a value that has changed.
        Unchanged repeats of the settings a device re-announces each cycle do
        not fire it, so a consumer is not woken several times for one cycle.

        It is called from the task reading the socket, with no data passed, so
        a consumer reads the current state with :meth:`get_devices`. Keep it
        short: slow work here holds up the socket.
        """
        self._listeners.append(listener)

        def remove() -> None:
            with contextlib.suppress(ValueError):
                self._listeners.remove(listener)

        return remove

    def get_devices(self) -> list[CoolbotDevice]:
        """Return every device on the account, from what has arrived so far.

        Synchronous so that a listener can publish without awaiting.
        """
        return build_devices(self._profile_records, self._pins, self._live_at)

    async def async_get_devices(
        self, *, wait_for_live: bool = True, timeout: float | None = None
    ) -> list[CoolbotDevice]:
        """Return every device on the account.

        With ``wait_for_live`` set, waits for a live temperature push so the
        returned readings are known to be current rather than replayed cache.
        Falls back to the snapshot if nothing arrives in time; callers can tell
        the difference from ``data_age_seconds`` being None.
        """
        if wait_for_live and not self._live_push.is_set():
            # Allow a couple of push intervals before giving up.
            budget = timeout if timeout is not None else PUSH_INTERVAL_SECONDS * 3
            try:
                await asyncio.wait_for(self._live_push.wait(), budget)
            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "no live push within %.0fs; readings may be a replayed snapshot", budget
                )

        return self.get_devices()

    async def async_refresh_profile(self) -> None:
        """Re-read the account profile and resubscribe to its dashboards.

        The profile is otherwise read once, while connecting, so a cooler added
        to or removed from the account is not noticed for as long as the
        connection lasts. Subscribing again matters as much as the re-read: it
        is what makes the server replay pin values, so without it a cooler that
        has just appeared would have no MAC address to be identified by, and a
        slot handed to different hardware would keep answering with the old
        unit's values.
        """
        await self._load_profile()
        await self._sync_dashboards()

    async def _sync_dashboards(self) -> None:
        """Subscribe to every dashboard and wait for the replayed pins.

        Subscribing makes the server replay every pin value it holds and start
        forwarding live updates. The replay arrives as a burst of separate
        frames, so this waits until each slot the profile says has connected
        before has replayed its MAC address. Returning any earlier hands the
        caller a slot still carrying its previous occupant's identity.
        """
        self._replay_expected = self._connected_targets()
        self._replay_seen = set()
        self._replay_done.clear()
        self._snapshot_ready.clear()
        self._awaiting_identity = set(self._replay_expected)
        self._held = {}

        try:
            await self._await_replay()
        finally:
            # Whatever is still held never had its identity confirmed, so it
            # cannot be attributed to any cooler. Later pins flow as usual.
            self._awaiting_identity = set()
            self._held = {}

    async def _await_replay(self) -> None:
        """Send the subscriptions and wait for every expected MAC to replay."""
        for dash_id in self._dash_ids():
            await self._send(CMD_APP_SYNC, str(dash_id))

        if not self._replay_expected:
            # An account with no cooler that has ever connected has nothing to
            # replay, and waiting would only stall for the whole timeout.
            return

        try:
            await asyncio.wait_for(self._replay_done.wait(), self._request_timeout)
        except asyncio.TimeoutError:
            # Whatever did not answer cannot be vouched for: a slot reassigned
            # to different hardware would otherwise go on offering the previous
            # occupant's MAC, and be taken for it. Better unidentified than
            # wrong; the caller sees a device it cannot name yet. Everything
            # cached goes, not just the MAC: readings left behind would be
            # married to whichever MAC arrives later, and the previous
            # occupant's temperatures must not surface under its replacement.
            unconfirmed = self._replay_expected - self._replay_seen
            for target in unconfirmed:
                self._pins.pop(target, None)
                self._live_at.pop(target, None)
            _LOGGER.warning(
                "pins replayed for %d of %d devices after subscribing; "
                "%d left unidentified",
                len(self._replay_seen & self._replay_expected),
                len(self._replay_expected),
                len(unconfirmed),
            )

    def _connected_targets(self) -> set[str]:
        """Slots the profile says hold a cooler that has connected before."""
        return {
            target_for(dash.dash_id, device_id)
            for dash in merge_dashboards(self._profile_records)
            for device_id, raw in dash.devices.items()
            if raw.get("connectTime")
        }

    async def _load_profile(self) -> None:
        """Ask for the account profile and wait for it to arrive.

        It comes back as its own frame rather than as a response body.
        """
        self._profile_ready.clear()
        await self._send(CMD_LOAD_PROFILE_GZIPPED)
        try:
            await asyncio.wait_for(self._profile_ready.wait(), self._request_timeout)
        except asyncio.TimeoutError as err:
            raise CoolbotConnectionError("timed out waiting for the account profile") from err

    async def async_ping(self) -> None:
        """Keep the connection alive."""
        status = await self._request(CMD_PING)
        if status != STATUS_OK:
            raise CoolbotConnectionError(f"ping failed: {status_message(status)}")

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._ws.closed

    # --- internals ----------------------------------------------------------

    def _invalidate_reassigned_slot(self, target: str, mac: str) -> None:
        """Drop a slot's cached state when its MAC changes.

        A different MAC means the slot has been handed to other hardware. The
        rest of the cached pins - temperatures, set point, hardware revisions -
        and the live timestamp still describe the previous occupant, and the
        replay delivers pins in no guaranteed order, so keeping them until the
        new unit's own values arrive would publish the old readings, marked
        current, under the new identity.
        """
        known = self._pins.get(target, {}).get(PIN_MAC_ADDRESS)
        if known is None or known == mac:
            return
        self._pins[target] = {}
        self._live_at.pop(target, None)

    def _forget_absent_slots(self, records: list[dict[str, Any]]) -> None:
        """Drop cached pins for slots a new profile no longer holds.

        The service keeps serving plausible values for slots that are empty, so
        pins left over from a cooler that has been removed would otherwise go on
        answering for a slot it no longer occupies.
        """
        keep = {
            target_for(dash.dash_id, device_id)
            for dash in merge_dashboards(records)
            for device_id in dash.devices
        }
        for target in self._pins.keys() - keep:
            self._pins.pop(target, None)
            self._live_at.pop(target, None)

    def _dash_ids(self) -> list[int]:
        seen: dict[int, None] = {}
        for record in self._profile_records:
            dash_id = record.get("id")
            if dash_id is not None:
                seen.setdefault(dash_id, None)
        return list(seen)

    async def _send(self, cmd: int, body: bytes | str = b"") -> int:
        msg_id = next(self._ids)
        await self._send_frame(cmd, msg_id, body)
        return msg_id

    async def _send_frame(self, cmd: int, msg_id: int, body: bytes | str = b"") -> None:
        """Write one frame, presenting any socket failure as our own error.

        Callers handle ``CoolbotError``; letting an aiohttp writer error escape
        raw would bypass their reconnect and cleanup paths.
        """
        if self._ws is None or self._ws.closed:
            raise CoolbotConnectionError("socket is not open")
        try:
            await self._ws.send_bytes(encode_frame(cmd, msg_id, body))
        except (aiohttp.ClientError, OSError) as err:
            raise CoolbotConnectionError(f"could not send command {cmd}: {err}") from err

    async def _request(self, cmd: int, body: bytes | str = b"") -> int:
        """Send a frame and wait for the RESPONSE carrying its status."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[int] = loop.create_future()
        # Registered before sending: sending yields to the event loop, so the
        # reader can receive the response before this coroutine resumes, and an
        # unregistered response is discarded and then waited out in full.
        msg_id = next(self._ids)
        self._responses[msg_id] = future
        try:
            await self._send_frame(cmd, msg_id, body)
            return await asyncio.wait_for(future, self._request_timeout)
        except asyncio.TimeoutError as err:
            raise CoolbotConnectionError(f"no response to command {cmd}") from err
        finally:
            self._responses.pop(msg_id, None)
            _discard(future)

    async def _read_loop(self) -> None:
        assert self._ws is not None
        try:
            async for message in self._ws:
                if message.type is aiohttp.WSMsgType.BINARY:
                    for frame in decode_frames(message.data):
                        self._handle(frame)
                elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a reader crash must not be silent
            _LOGGER.exception("websocket reader failed")
        finally:
            self._fail_pending()

    def _fail_pending(self) -> None:
        if self._closed:
            return
        for future in self._responses.values():
            if not future.done():
                future.set_exception(CoolbotConnectionError("connection closed"))
        self._responses.clear()

    def _handle(self, frame: Frame) -> None:
        if frame.cmd == CMD_RESPONSE:
            future = self._responses.get(frame.msg_id)
            if future is not None and not future.done():
                future.set_result(frame.status or 0)
            return

        if frame.cmd in COMPRESSED_CMDS:
            self._handle_profile(frame)
            return

        if frame.cmd in (CMD_HARDWARE, CMD_APP_SYNC):
            update = parse_pin_update(frame.body)
            if update is None:
                return
            received_at = datetime.now(UTC)
            if update.pin != PIN_MAC_ADDRESS and update.target in self._awaiting_identity:
                # Until the MAC replays, the slot still carries whatever MAC it
                # had before, which may belong to hardware since replaced.
                # Storing this now would pair the new unit's readings with the
                # old unit's identity for anyone reading in the meantime.
                self._held.setdefault(update.target, []).append(
                    (frame.cmd, update, received_at)
                )
                return
            self._apply_pin(frame.cmd, update, received_at)

    def _apply_pin(self, cmd: int, update: PinUpdate, received_at: datetime) -> None:
        """Store one pin value, and tell listeners if it is news."""
        if update.pin == PIN_MAC_ADDRESS:
            self._invalidate_reassigned_slot(update.target, update.value)
        previous = self._pins.get(update.target, {}).get(update.pin)
        self._pins.setdefault(update.target, {})[update.pin] = update.value
        self._snapshot_ready.set()

        # Specifically the MAC: it is what a device is identified by, and a
        # slot's other pins can replay before it. Settling for any pin
        # would let a caller read the previous occupant's identity.
        if update.pin == PIN_MAC_ADDRESS:
            self._replay_seen.add(update.target)
            if self._replay_expected and self._replay_expected <= self._replay_seen:
                self._replay_done.set()
            if update.target in self._awaiting_identity:
                # Identity confirmed: what arrived meanwhile now belongs to it,
                # in the order it arrived.
                self._awaiting_identity.discard(update.target)
                for held_cmd, held, held_at in self._held.pop(update.target, []):
                    self._apply_pin(held_cmd, held, held_at)
        # Only a HARDWARE push proves the value is current; APP_SYNC is the
        # server replaying whatever it had cached when we connected, so a
        # replay must not wake listeners either.
        if cmd != CMD_HARDWARE:
            return
        if update.pin in LIVE_PINS:
            self._live_at[update.target] = received_at
            self._live_push.set()
            # Freshness moved on even if the temperature itself held steady,
            # and a consumer gating availability on age needs to hear that.
            self._notify()
        elif previous != update.value:
            self._notify()

    def _notify(self) -> None:
        """Tell listeners that a device has pushed something new.

        Runs on the task reading the socket, so a listener that raises must not
        be allowed to take the connection down with it. The list is copied
        because a listener is free to remove itself while being called.
        """
        for listener in list(self._listeners):
            try:
                listener()
            except Exception:  # noqa: BLE001 - one bad listener must not stop the rest
                _LOGGER.exception("update listener failed")

    def _handle_profile(self, frame: Frame) -> None:
        payload = inflate_json(frame.body)
        if payload is None:
            _LOGGER.debug("could not inflate frame cmd=%s", frame.cmd)
            return

        if isinstance(payload, dict) and "dashBoards" in payload:
            records = payload.get("dashBoards") or []
        elif isinstance(payload, dict):
            records = [payload]  # a single-dashboard fetch
        else:
            return

        fresh = [r for r in records if isinstance(r, dict)]
        if frame.cmd == CMD_LOAD_PROFILE_GZIPPED:
            self._forget_absent_slots(fresh)
            # A full profile load is the authoritative account state, so it
            # replaces what came before. Appending would keep a cooler that has
            # since been removed, because dashboards are merged by id and the
            # older record would go on contributing it.
            self._profile_records = fresh
            self._profile_ready.set()
            return

        # A per-dashboard fetch describes one dashboard and is merged with the
        # full profile rather than replacing it.
        self._profile_records.extend(fresh)
        if frame.cmd == CMD_DASH_GZIPPED:
            self._profile_ready.set()
