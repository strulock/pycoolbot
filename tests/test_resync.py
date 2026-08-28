"""Resubscribing after a profile re-read, and what that does to cached pins."""

from __future__ import annotations

import asyncio
import json
import zlib
from typing import Any

from pycoolbot import CoolbotClient, build_devices
from pycoolbot.const import (
    CMD_APP_SYNC,
    CMD_HARDWARE,
    CMD_LOAD_PROFILE_GZIPPED,
    PIN_MAC_ADDRESS,
    PIN_ROOM_TEMP,
)
from pycoolbot.protocol import Frame, decode_frames


def _profile(*devices: tuple[int, int]) -> dict[str, Any]:
    """Build a profile from (device_id, connect_time) pairs."""
    return {
        "dashBoards": [
            {
                "id": 10,
                "name": "CoolBot",
                "devices": [
                    {"id": device_id, "name": f"Cooler {device_id}", "connectTime": ct}
                    for device_id, ct in devices
                ],
            }
        ]
    }


def _pin_body(target: str, pin: str, value: str) -> bytes:
    return f"{target}\0vw\0{pin}\0{value}".encode()


class _Service:
    """A stand-in for the cloud: serves a profile and replays pins on sync."""

    closed = False

    def __init__(self, client: CoolbotClient) -> None:
        self._client = client
        self.profile = _profile((0, 1))
        #: Pins the service will replay per slot, keyed by target.
        self.pins: dict[str, dict[str, str]] = {
            "10": {PIN_MAC_ADDRESS: "AA:AA:AA:AA:AA:AA", PIN_ROOM_TEMP: "38.5"}
        }
        self.syncs: list[str] = []

    async def send_bytes(self, data: bytes) -> None:
        await asyncio.sleep(0)
        for frame in decode_frames(data):
            if frame.cmd == CMD_LOAD_PROFILE_GZIPPED:
                self._client._handle(
                    Frame(
                        cmd=CMD_LOAD_PROFILE_GZIPPED,
                        msg_id=frame.msg_id,
                        body=zlib.compress(json.dumps(self.profile).encode()),
                    )
                )
            elif frame.cmd == CMD_APP_SYNC:
                dash = frame.body.decode()
                self.syncs.append(dash)
                for target, pins in self.pins.items():
                    if target.split("-")[0] != dash:
                        continue
                    for pin, value in pins.items():
                        self._client._handle(
                            Frame(
                                cmd=CMD_HARDWARE,
                                msg_id=0,
                                body=_pin_body(target, pin, value),
                            )
                        )


def _client() -> CoolbotClient:
    return CoolbotClient("user@example.com", "hunter2", request_timeout=0.5)


def _by_unique_id(client: CoolbotClient) -> dict[str, Any]:
    devices = build_devices(client._profile_records, client._pins, client._live_at)
    return {device.unique_id: device for device in devices}


def test_a_cooler_added_later_is_subscribed_and_identified() -> None:
    """A refresh has to resubscribe, or a new cooler never gets its MAC.

    Subscribing is what makes the server replay pins, and the MAC is what a
    device is identified by, so a profile re-read on its own would leave the
    new cooler unidentifiable for the life of the connection.
    """

    async def scenario() -> tuple[set[str], list[str]]:
        client = _client()
        service = _Service(client)
        client._ws = service

        await client.async_refresh_profile()
        assert set(_by_unique_id(client)) == {"coolbot_aaaaaaaaaaaa"}

        # A second cooler is added to the account, on the same dashboard.
        service.profile = _profile((0, 1), (1, 2))
        service.pins["10-1"] = {
            PIN_MAC_ADDRESS: "BB:BB:BB:BB:BB:BB",
            PIN_ROOM_TEMP: "41.0",
        }
        service.syncs.clear()

        await client.async_refresh_profile()
        return set(_by_unique_id(client)), service.syncs

    unique_ids, syncs = asyncio.run(scenario())
    assert unique_ids == {"coolbot_aaaaaaaaaaaa", "coolbot_bbbbbbbbbbbb"}
    assert syncs == ["10"]


def test_a_reused_slot_does_not_inherit_the_old_identity() -> None:
    """Different hardware in the same slot must not answer as its predecessor.

    Pins are keyed by slot, so the previous cooler's cached MAC would otherwise
    publish the replacement under the old identity, complete with the old
    readings.
    """

    async def scenario() -> dict[str, Any]:
        client = _client()
        service = _Service(client)
        client._ws = service

        await client.async_refresh_profile()
        assert set(_by_unique_id(client)) == {"coolbot_aaaaaaaaaaaa"}

        # The cooler is replaced by different hardware in the same slot.
        service.profile = _profile((0, 99))
        service.pins["10"] = {
            PIN_MAC_ADDRESS: "CC:CC:CC:CC:CC:CC",
            PIN_ROOM_TEMP: "45.0",
        }

        await client.async_refresh_profile()
        return _by_unique_id(client)

    devices = asyncio.run(scenario())
    assert set(devices) == {"coolbot_cccccccccccc"}
    assert devices["coolbot_cccccccccccc"].room_temp_f == 45.0


class _LateMacService(_Service):
    """Replays a slot's temperature promptly and its MAC a beat later.

    The real burst arrives as separate frames in no guaranteed order, so a
    caller that stops waiting at the first pin can still be holding the
    previous occupant's identity.
    """

    async def send_bytes(self, data: bytes) -> None:
        await asyncio.sleep(0)
        for frame in decode_frames(data):
            if frame.cmd == CMD_LOAD_PROFILE_GZIPPED:
                self._client._handle(
                    Frame(
                        cmd=CMD_LOAD_PROFILE_GZIPPED,
                        msg_id=frame.msg_id,
                        body=zlib.compress(json.dumps(self.profile).encode()),
                    )
                )
            elif frame.cmd == CMD_APP_SYNC:
                self.syncs.append(frame.body.decode())
                for target, pins in self.pins.items():
                    for pin, value in pins.items():
                        if pin == PIN_MAC_ADDRESS:
                            continue
                        self._client._handle(
                            Frame(cmd=CMD_HARDWARE, msg_id=0, body=_pin_body(target, pin, value))
                        )
                asyncio.get_running_loop().call_soon(self._replay_macs)

    def _replay_macs(self) -> None:
        for target, pins in self.pins.items():
            if PIN_MAC_ADDRESS in pins:
                self._client._handle(
                    Frame(
                        cmd=CMD_HARDWARE,
                        msg_id=0,
                        body=_pin_body(target, PIN_MAC_ADDRESS, pins[PIN_MAC_ADDRESS]),
                    )
                )


def test_the_wait_holds_out_for_the_identifying_pin() -> None:
    """Other pins landing first must not end the wait.

    A slot's temperature can replay before its MAC, and stopping there would
    hand back the previous occupant's identity with the new occupant's
    readings.
    """

    async def scenario() -> dict[str, Any]:
        client = _client()
        service = _LateMacService(client)
        client._ws = service

        await client.async_refresh_profile()
        assert set(_by_unique_id(client)) == {"coolbot_aaaaaaaaaaaa"}

        service.profile = _profile((0, 99))
        service.pins["10"] = {
            PIN_MAC_ADDRESS: "CC:CC:CC:CC:CC:CC",
            PIN_ROOM_TEMP: "45.0",
        }

        await client.async_refresh_profile()
        return _by_unique_id(client)

    assert set(asyncio.run(scenario())) == {"coolbot_cccccccccccc"}


def test_pins_for_a_removed_slot_are_forgotten() -> None:
    """A slot that leaves the profile must not keep answering."""

    async def scenario() -> dict[str, dict[str, str]]:
        client = _client()
        service = _Service(client)
        client._ws = service

        await client.async_refresh_profile()
        assert "10" in client._pins

        service.profile = {"dashBoards": [{"id": 10, "name": "CoolBot", "devices": []}]}
        service.pins.clear()
        await client.async_refresh_profile()
        return client._pins

    assert asyncio.run(scenario()) == {}


def test_an_account_with_nothing_connected_does_not_stall() -> None:
    """Waiting for a replay that cannot come would cost the whole timeout."""

    async def scenario() -> None:
        client = _client()
        service = _Service(client)
        service.profile = _profile((0, 0))  # never connected
        service.pins.clear()
        client._ws = service

        await asyncio.wait_for(client.async_refresh_profile(), 0.3)

    asyncio.run(scenario())
