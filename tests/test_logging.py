"""What the library is allowed to put in a log."""

from __future__ import annotations

import re
from pathlib import Path

import pycoolbot.client


def test_no_log_message_carries_an_account_identifier() -> None:
    """Debug logs are shared when people ask for help.

    Consumers expose this logger to their users - Home Assistant lists it in
    the integration manifest - so an account address or credential reaching a
    log line reaches whoever that log is sent to. Checks the arguments of every
    logging call rather than the whole module, since the fields legitimately
    appear elsewhere.
    """
    source = Path(pycoolbot.client.__file__).read_text(encoding="utf-8")
    logged = " ".join(re.findall(r"_LOGGER\.\w+\((.*?)\)", source, flags=re.DOTALL))

    assert logged, "no logging calls found; the check would be vacuous"
    for forbidden in ("self._email", "self._password", "hash_password", "login_body"):
        assert forbidden not in logged, f"{forbidden} must not be logged"
