"""Tests for device enumeration and the phantom-device rules."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pycoolbot.const import PIN_MAC_ADDRESS, PIN_ROOM_TEMP, PIN_SET_POINT
from pycoolbot.models import build_devices, merge_dashboards, target_for


def test_target_for_addresses_device_zero_by_bare_dash_id() -> None:
    assert target_for(123, 0) == "123"
    assert target_for(123, 2) == "123-2"


class TestMergeDashboards:
    def test_connected_record_beats_template_row(self) -> None:
        # The per-dashboard fetch carries a generic ESP8266 template device;
        # a record that has actually connected must win.
        template = {"id": 1, "devices": [{"id": 0, "name": "ESP8266"}]}
        real = {"id": 1, "devices": [{"id": 0, "name": "Cellar", "connectTime": 1700000000000}]}
        for order in ([template, real], [real, template]):
            (dash,) = merge_dashboards(order)
            assert dash.devices[0]["name"] == "Cellar"

    def test_records_without_ids_are_skipped(self) -> None:
        assert merge_dashboards([{"name": "no id"}]) == []
        assert merge_dashboards(None) == []


class TestBuildDevices:
    def _profile(self, **device) -> list[dict]:
        return [{"id": 10, "name": "Cooler", "devices": [{"id": 0, **device}]}]

    def test_unprovisioned_slot_reports_no_readings(self) -> None:
        # The server keeps plausible-looking stale pin state on empty slots;
        # readings must be withheld, with the withholding made visible.
        pins = {"10": {PIN_ROOM_TEMP: "56.570", PIN_SET_POINT: "40.0"}}
        (device,) = build_devices(self._profile(name="Empty slot"), pins)
        assert not device.is_provisioned
        assert device.room_temp_f is None
        assert device.set_point_f is None
        assert device.ignored_stale_pins == 2

    def test_connect_time_proves_provisioned(self) -> None:
        pins = {"10": {PIN_ROOM_TEMP: "38.1"}}
        (device,) = build_devices(self._profile(connectTime=1700000000000), pins)
        assert device.is_provisioned
        assert device.room_temp_f == 38.1

    def test_mac_proves_provisioned_and_pins_unique_id(self) -> None:
        pins = {"10": {PIN_MAC_ADDRESS: "AA:BB:CC:DD:EE:FF"}}
        (device,) = build_devices(self._profile(), pins)
        assert device.is_provisioned
        assert device.unique_id == "coolbot_aabbccddeeff"

    def test_unique_id_falls_back_to_slot_when_no_mac(self) -> None:
        (device,) = build_devices(self._profile(connectTime=1), {})
        assert device.unique_id == "coolbot_10_0"

    def test_recent_live_push_overrides_offline_snapshot(self) -> None:
        # The profile is a login-time snapshot; a device actively pushing data
        # is online whatever the snapshot claims.
        now = datetime.now(UTC)
        (device,) = build_devices(
            self._profile(connectTime=1, status="OFFLINE"),
            {"10": {PIN_ROOM_TEMP: "38.1"}},
            {"10": now - timedelta(seconds=5)},
            now=now,
        )
        assert device.available

    def test_stale_push_does_not_override_offline_snapshot(self) -> None:
        now = datetime.now(UTC)
        (device,) = build_devices(
            self._profile(connectTime=1, status="OFFLINE"),
            {"10": {PIN_ROOM_TEMP: "38.1"}},
            {"10": now - timedelta(minutes=10)},
            now=now,
        )
        assert not device.available

    def test_unparseable_reading_becomes_none(self) -> None:
        pins = {"10": {PIN_ROOM_TEMP: "ERR"}}
        (device,) = build_devices(self._profile(connectTime=1), pins)
        assert device.room_temp_f is None
