"""Push notification: when a listener fires, and when it deliberately does not."""

from __future__ import annotations

import logging

import pytest

from pycoolbot import CoolbotClient
from pycoolbot.const import (
    CMD_APP_SYNC,
    CMD_HARDWARE,
    PIN_FINS_TEMP,
    PIN_HARDWARE_STATUS,
    PIN_MAC_ADDRESS,
    PIN_ROOM_TEMP,
    PIN_WIFI_DBM,
)
from pycoolbot.protocol import Frame

TARGET = "325753021"


def pin_frame(cmd: int, pin: str, value: str, target: str = TARGET) -> Frame:
    return Frame(cmd=cmd, msg_id=1, body="\0".join([target, "vw", pin, value]).encode())


@pytest.fixture
def client() -> CoolbotClient:
    return CoolbotClient("user@example.com", "pw")


@pytest.fixture
def calls(client: CoolbotClient) -> list[int]:
    seen: list[int] = []
    client.add_listener(lambda: seen.append(1))
    return seen


def test_live_reading_notifies(client: CoolbotClient, calls: list[int]) -> None:
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    assert len(calls) == 1


def test_unchanged_reading_still_notifies(client: CoolbotClient, calls: list[int]) -> None:
    """Freshness moved on even though the temperature held steady.

    A consumer that marks a device unavailable once its readings stop arriving
    has to hear this, or a steady temperature would look like an outage.
    """
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    assert len(calls) == 2


def test_changed_setting_notifies(client: CoolbotClient, calls: list[int]) -> None:
    client._handle(pin_frame(CMD_HARDWARE, PIN_WIFI_DBM, "-47"))
    assert len(calls) == 1


def test_repeated_setting_does_not_notify(client: CoolbotClient, calls: list[int]) -> None:
    """Devices re-announce their settings every cycle; only changes are news."""
    client._handle(pin_frame(CMD_HARDWARE, PIN_HARDWARE_STATUS, "OK"))
    assert len(calls) == 1
    client._handle(pin_frame(CMD_HARDWARE, PIN_HARDWARE_STATUS, "OK"))
    client._handle(pin_frame(CMD_HARDWARE, PIN_HARDWARE_STATUS, "OK"))
    assert len(calls) == 1


def test_replayed_snapshot_does_not_notify(client: CoolbotClient, calls: list[int]) -> None:
    """APP_SYNC is the server replaying its cache, which can be minutes old."""
    for pin, value in (
        (PIN_ROOM_TEMP, "56.3"),
        (PIN_FINS_TEMP, "54.1"),
        (PIN_MAC_ADDRESS, "40:F5:20:27:BE:94"),
    ):
        client._handle(pin_frame(CMD_APP_SYNC, pin, value))
    assert calls == []


def test_a_burst_of_repeats_collapses(client: CoolbotClient, calls: list[int]) -> None:
    """One push cycle should not wake a consumer once per pin.

    A device re-sends its settings alongside each reading, so without the
    changed-value check a single cycle would fire the listener many times.
    """
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    for pin, value in (
        (PIN_HARDWARE_STATUS, "OK"),
        (PIN_WIFI_DBM, "-47"),
        (PIN_MAC_ADDRESS, "40:F5:20:27:BE:94"),
    ):
        client._handle(pin_frame(CMD_HARDWARE, pin, value))
    calls.clear()

    # Second cycle: the reading arrives again, the settings repeat unchanged.
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.5"))
    for pin, value in (
        (PIN_HARDWARE_STATUS, "OK"),
        (PIN_WIFI_DBM, "-47"),
        (PIN_MAC_ADDRESS, "40:F5:20:27:BE:94"),
    ):
        client._handle(pin_frame(CMD_HARDWARE, pin, value))
    assert len(calls) == 1


def test_every_listener_is_called(client: CoolbotClient) -> None:
    seen: list[str] = []
    client.add_listener(lambda: seen.append("a"))
    client.add_listener(lambda: seen.append("b"))
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    assert seen == ["a", "b"]


def test_remover_stops_the_callback(client: CoolbotClient) -> None:
    seen: list[int] = []
    remove = client.add_listener(lambda: seen.append(1))
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    remove()
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.9"))
    assert len(seen) == 1


def test_removing_twice_is_harmless(client: CoolbotClient) -> None:
    remove = client.add_listener(lambda: None)
    remove()
    remove()


def test_a_listener_may_remove_itself_while_called(client: CoolbotClient) -> None:
    """The list is iterated over a copy, so this must not skip the other one."""
    seen: list[str] = []
    remove: list[object] = []

    def first() -> None:
        seen.append("first")
        remove[0]()  # type: ignore[operator]

    remove.append(client.add_listener(first))
    client.add_listener(lambda: seen.append("second"))

    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))
    assert seen == ["first", "second"]
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.9"))
    assert seen == ["first", "second", "second"]


def test_a_raising_listener_does_not_stop_the_others(
    client: CoolbotClient, caplog: pytest.LogCaptureFixture
) -> None:
    """A consumer's bug must not take the socket down with it."""
    seen: list[str] = []
    client.add_listener(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    client.add_listener(lambda: seen.append("after"))

    with caplog.at_level(logging.ERROR, logger="pycoolbot.client"):
        client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3"))

    assert seen == ["after"]
    assert "update listener failed" in caplog.text


def test_notifications_are_per_device(client: CoolbotClient, calls: list[int]) -> None:
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "56.3", target=TARGET))
    client._handle(pin_frame(CMD_HARDWARE, PIN_ROOM_TEMP, "41.0", target=f"{TARGET}-1"))
    assert len(calls) == 2


def test_get_devices_is_synchronous(client: CoolbotClient) -> None:
    """A listener publishes without awaiting, so this must not be a coroutine."""
    client._handle(pin_frame(CMD_APP_SYNC, PIN_ROOM_TEMP, "56.3"))
    assert client.get_devices() == []  # no profile loaded, so nothing to report
