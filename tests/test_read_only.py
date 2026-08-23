"""The library's core promise: it can observe a cooler but never change one."""

from __future__ import annotations

import re
from pathlib import Path

import pycoolbot.client


def test_only_read_only_commands_are_ever_sent() -> None:
    """A pin write would be an outbound HARDWARE frame; there must be none.

    Checks the actual send call sites rather than mentions of the constant,
    which legitimately appears where inbound frames are decoded.
    """
    source = Path(pycoolbot.client.__file__).read_text(encoding="utf-8")
    sent = set(re.findall(r"await self\._(?:send|request)\(\s*(CMD_\w+)", source))

    allowed = {"CMD_LOGIN", "CMD_LOAD_PROFILE_GZIPPED", "CMD_APP_SYNC", "CMD_PING"}
    assert sent, "no send call sites found; the check would be vacuous"
    assert sent <= allowed, f"unexpected outbound command(s): {sent - allowed}"
