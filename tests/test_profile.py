"""The account profile: what a fresh load replaces, and what it merges."""

from __future__ import annotations

import asyncio
import json
import zlib
from typing import Any

import pytest

from pycoolbot import CoolbotClient, CoolbotConnectionError, build_devices
from pycoolbot.const import CMD_DASH_GZIPPED, CMD_LOAD_PROFILE_GZIPPED
from pycoolbot.protocol import Frame


def _profile(*device_ids: int) -> dict[str, Any]:
    """Build an account profile holding one dashboard with these devices."""
    return {
        "dashBoards": [
            {
                "id": 10,
                "name": "CoolBot",
                "devices": [
                    {"id": device_id, "name": f"Cooler {device_id}", "connectTime": 1}
                    for device_id in device_ids
                ],
            }
        ]
    }


def _frame(cmd: int, payload: dict[str, Any]) -> Frame:
    return Frame(cmd=cmd, msg_id=1, body=zlib.compress(json.dumps(payload).encode()))


def _client() -> CoolbotClient:
    return CoolbotClient("user@example.com", "hunter2", request_timeout=0.5)


def _device_ids(client: CoolbotClient) -> set[int]:
    """Return the device ids the client would currently report."""
    devices = build_devices(client._profile_records, client._pins, client._live_at)
    return {device.device_id for device in devices}


def test_a_full_profile_load_replaces_the_previous_one() -> None:
    """A cooler removed from the account has to actually disappear.

    Dashboards are merged by id, so appending would leave the older record
    contributing the removed device for the life of the connection.
    """
    client = _client()
    client._handle(_frame(CMD_LOAD_PROFILE_GZIPPED, _profile(0, 1)))
    assert _device_ids(client) == {0, 1}

    client._handle(_frame(CMD_LOAD_PROFILE_GZIPPED, _profile(0)))

    assert _device_ids(client) == {0}


def test_a_dashboard_fetch_merges_with_the_profile() -> None:
    """One dashboard's own fetch describes it, so it must not replace the rest."""
    client = _client()
    client._handle(_frame(CMD_LOAD_PROFILE_GZIPPED, _profile(0)))
    client._handle(_frame(CMD_DASH_GZIPPED, _profile(1)))

    assert _device_ids(client) == {0, 1}


class _ProfileSocket:
    """Answers a profile request with whatever the test currently serves."""

    closed = False

    def __init__(self, client: CoolbotClient) -> None:
        self._client = client
        self.payload = _profile(0, 1)
        self.requests = 0

    async def send_bytes(self, data: bytes) -> None:
        await asyncio.sleep(0)
        self.requests += 1
        self._client._handle(_frame(CMD_LOAD_PROFILE_GZIPPED, self.payload))


def test_refreshing_the_profile_picks_up_a_removed_cooler() -> None:
    """Without a re-read, the device list stays frozen at connect time."""

    async def scenario() -> tuple[set[int], set[int], int]:
        client = _client()
        socket = _ProfileSocket(client)
        client._ws = socket

        await client.async_refresh_profile()
        before = _device_ids(client)

        socket.payload = _profile(0)
        await client.async_refresh_profile()
        after = _device_ids(client)

        return before, after, socket.requests

    before, after, requests = asyncio.run(scenario())
    assert before == {0, 1}
    assert after == {0}
    assert requests == 2


def test_refreshing_without_an_answer_is_a_connection_error() -> None:
    """A profile that never arrives must not hang the caller forever."""

    class _SilentSocket:
        closed = False

        async def send_bytes(self, data: bytes) -> None:
            return None

    async def scenario() -> None:
        client = _client()
        client._ws = _SilentSocket()
        with pytest.raises(CoolbotConnectionError, match="profile"):
            await client.async_refresh_profile()

    asyncio.run(scenario())
