"""Device enumeration.

The rule this module exists to enforce: the server keeps stale pin state for
device slots, and a newly added CoolBot lands on a slot that may already hold
leftovers. Those leftovers look completely plausible. Verified against a live
account, an unused slot served a frozen 82.742 F for over an hour, and later two
unused slots both reported 56.570 F — a believable cellar reading — for devices
that had never connected to anything. Deleting the test devices did not clear the
streams; they kept reporting.

So readings are reported only for devices proven to be real, and enumeration is
driven by the account profile rather than by whatever streams happen to appear. A
stream-driven design would create permanent phantom devices that the user has no
way to remove.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .const import (
    PUSH_INTERVAL_SECONDS,
    PIN_COOLBOT_HARDWARE,
    PIN_FINS_TEMP,
    PIN_HARDWARE_STATUS,
    PIN_JUMPER_FIRMWARE,
    PIN_JUMPER_HARDWARE,
    PIN_MAC_ADDRESS,
    PIN_NOTIFY_HIGH,
    PIN_NOTIFY_LOW,
    PIN_ROOM_TEMP,
    PIN_SET_POINT,
    PIN_WIFI_DBM,
)


def target_for(dash_id: int | str, device_id: int) -> str:
    """Blynk addresses device 0 by the bare dash id and the rest with a -N suffix."""
    return str(dash_id) if device_id == 0 else f"{dash_id}-{device_id}"


@dataclass(slots=True)
class CoolbotDevice:
    """One CoolBot as it should be presented to a consumer.

    ``is_provisioned`` distinguishes real hardware from an empty device slot.
    When it is False every reading is None regardless of what the slot's stream
    held — see the module docstring.
    """

    dash_id: int
    device_id: int
    target: str
    name: str
    unique_id: str
    status: str | None = None
    mac_address: str | None = None
    is_provisioned: bool = False
    available: bool = False

    hardware_status: str | None = None
    set_point_f: float | None = None
    room_temp_f: float | None = None
    fins_temp_f: float | None = None
    wifi_dbm: float | None = None
    notify_low_f: float | None = None
    notify_high_f: float | None = None
    jumper_firmware: str | None = None
    jumper_hardware: str | None = None
    coolbot_hardware: str | None = None

    last_disconnect: datetime | None = None
    #: When a live measurement last arrived for this device, if one did.
    last_data_at: datetime | None = None
    #: Pin count held by an unprovisioned slot, surfaced rather than hidden.
    ignored_stale_pins: int | None = None

    @property
    def data_age_seconds(self) -> float | None:
        """Seconds since a live push, or None if none arrived."""
        if self.last_data_at is None:
            return None
        return round((datetime.now(UTC) - self.last_data_at).total_seconds(), 1)


@dataclass(slots=True)
class _Dashboard:
    dash_id: int
    name: str | None
    devices: dict[int, dict[str, Any]] = field(default_factory=dict)


def merge_dashboards(records: list[dict[str, Any]] | None) -> list[_Dashboard]:
    """Combine the dashboard records seen on the socket.

    The full profile and the per-dashboard fetch describe the same dashboard, and
    the latter carries a generic ``ESP8266`` template device. Records that have
    actually connected win, which keeps the real names and drops template rows.
    """
    dashes: dict[int, _Dashboard] = {}

    for record in records or []:
        dash_id = record.get("id")
        if dash_id is None:
            continue
        dash = dashes.setdefault(dash_id, _Dashboard(dash_id=dash_id, name=record.get("name")))

        for device in record.get("devices") or []:
            device_id = device.get("id")
            if device_id is None:
                continue
            existing = dash.devices.get(device_id)
            if existing is None or (not existing.get("connectTime") and device.get("connectTime")):
                dash.devices[device_id] = device

    return list(dashes.values())


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _epoch_ms_to_dt(value: Any) -> datetime | None:
    if not value:  # also rejects 0, meaning "never"
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def build_devices(
    records: list[dict[str, Any]] | None,
    pins_by_target: dict[str, dict[str, str]] | None,
    live_at_by_target: dict[str, datetime] | None = None,
    *,
    now: datetime | None = None,
    live_within_seconds: float = PUSH_INTERVAL_SECONDS * 3,
) -> list[CoolbotDevice]:
    """Build the device list from the account profile plus observed pin state.

    Availability trusts a recent live push over the profile's status field. The
    profile is a snapshot taken at login, and a CoolBot that reconnects every few
    minutes is regularly caught mid-cycle: observed in practice, one run reported
    OFFLINE while simultaneously receiving temperature pushes 0.0 seconds old.
    Believing the snapshot would flap entities to unavailable for no reason, so a
    device actively sending data is treated as online whatever the snapshot says.
    """
    pins_by_target = pins_by_target or {}
    live_at_by_target = live_at_by_target or {}
    now = now or datetime.now(UTC)
    devices: list[CoolbotDevice] = []

    for dash in merge_dashboards(records):
        for device_id in sorted(dash.devices):
            raw = dash.devices[device_id]
            target = target_for(dash.dash_id, device_id)
            pins = pins_by_target.get(target, {})
            mac = pins.get(PIN_MAC_ADDRESS)

            # Real hardware either has connected before, or is reporting a MAC.
            # Placeholder slots never report a MAC.
            has_connected = bool(raw.get("connectTime"))
            is_provisioned = has_connected or bool(mac)
            status = raw.get("status")

            live_at = live_at_by_target.get(target)
            pushing_now = (
                live_at is not None and (now - live_at).total_seconds() <= live_within_seconds
            )
            available = is_provisioned and (
                str(status or "").upper() == "ONLINE" or pushing_now
            )

            entry = CoolbotDevice(
                dash_id=dash.dash_id,
                device_id=device_id,
                target=target,
                name=raw.get("name") or f"CoolBot {device_id}",
                # Prefer the MAC so entity ids survive the device being renamed.
                unique_id=(
                    f"coolbot_{mac.replace(':', '').lower()}"
                    if mac
                    else f"coolbot_{dash.dash_id}_{device_id}"
                ),
                status=status,
                mac_address=mac,
                is_provisioned=is_provisioned,
                available=available,
                last_disconnect=_epoch_ms_to_dt(raw.get("disconnectTime")),
            )

            if is_provisioned:
                entry.hardware_status = pins.get(PIN_HARDWARE_STATUS)
                entry.set_point_f = _to_float(pins.get(PIN_SET_POINT))
                entry.room_temp_f = _to_float(pins.get(PIN_ROOM_TEMP))
                entry.fins_temp_f = _to_float(pins.get(PIN_FINS_TEMP))
                entry.wifi_dbm = _to_float(pins.get(PIN_WIFI_DBM))
                entry.notify_low_f = _to_float(pins.get(PIN_NOTIFY_LOW))
                entry.notify_high_f = _to_float(pins.get(PIN_NOTIFY_HIGH))
                entry.jumper_firmware = pins.get(PIN_JUMPER_FIRMWARE)
                entry.jumper_hardware = pins.get(PIN_JUMPER_HARDWARE)
                entry.coolbot_hardware = pins.get(PIN_COOLBOT_HARDWARE)
                entry.last_data_at = live_at_by_target.get(target)
            else:
                # Readings stay None. Record that the slot held something so the
                # situation is visible instead of silently discarded.
                entry.ignored_stale_pins = len(pins) or None

            devices.append(entry)

    return devices
