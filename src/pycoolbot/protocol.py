"""Frame encoding, decoding, and credential hashing for the Blynk protocol."""

from __future__ import annotations

import base64
import hashlib
import json
import struct
import zlib
from dataclasses import dataclass
from typing import Any

from .const import (
    APP_NAME,
    APP_OS,
    APP_VERSION_CODE,
    CMD_RESPONSE,
    STATUS_MESSAGES,
)

_HEADER = struct.Struct(">BHH")
HEADER_SIZE = _HEADER.size


@dataclass(frozen=True, slots=True)
class Frame:
    """One decoded Blynk frame.

    ``status`` is set only for RESPONSE frames, where the header's length field
    carries a status code and there is no body.
    """

    cmd: int
    msg_id: int
    body: bytes = b""
    status: int | None = None


def hash_password(email: str, password: str) -> str:
    """Derive the login hash the app sends.

    Read directly out of the app bundle, which does::

        h = SHA256(); h.update(email.toLowerCase()); inner = h.finalize()
        h = SHA256(); h.update(password); h.update(inner); return base64(h.finalize())

    The inner digest is appended as **raw bytes**, not as a hex string — the
    difference is easy to get wrong and produces a hash the server rejects.
    """
    inner = hashlib.sha256(email.lower().encode("utf-8")).digest()
    outer = hashlib.sha256(password.encode("utf-8") + inner).digest()
    return base64.b64encode(outer).decode("ascii")


def login_body(email: str, password: str) -> bytes:
    """Build the LOGIN frame body."""
    fields = [email, hash_password(email, password), APP_OS, APP_VERSION_CODE, APP_NAME]
    return "\0".join(fields).encode("utf-8")


def encode_frame(cmd: int, msg_id: int, body: bytes | str = b"") -> bytes:
    """Serialize one frame."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    return _HEADER.pack(cmd, msg_id, len(body)) + body


def decode_frames(buf: bytes) -> list[Frame]:
    """Split a WebSocket payload into frames.

    A single payload can carry several frames back to back. A truncated trailing
    frame is dropped rather than guessed at.
    """
    frames: list[Frame] = []
    offset = 0
    total = len(buf)

    while offset + HEADER_SIZE <= total:
        cmd, msg_id, length = _HEADER.unpack_from(buf, offset)

        if cmd == CMD_RESPONSE:
            # No body: the length field is a status code.
            frames.append(Frame(cmd=cmd, msg_id=msg_id, status=length))
            offset += HEADER_SIZE
            continue

        end = offset + HEADER_SIZE + length
        if end > total:
            break  # incomplete frame; wait for more data
        frames.append(Frame(cmd=cmd, msg_id=msg_id, body=buf[offset + HEADER_SIZE : end]))
        offset = end

    return frames


def status_message(status: int) -> str:
    """Human-readable form of a RESPONSE status code."""
    return STATUS_MESSAGES.get(status, f"status {status}")


@dataclass(frozen=True, slots=True)
class PinUpdate:
    """A ``target|vw|pin|value`` message.

    ``target`` is the dash id for device 0 and ``<dashId>-<n>`` for the rest.
    """

    target: str
    pin: str
    value: str


def parse_pin_update(body: bytes) -> PinUpdate | None:
    """Parse a HARDWARE or APP_SYNC body, or return None if it is not a pin write."""
    parts = body.decode("utf-8", errors="replace").split("\0")
    # Only virtual writes carry readings; anything else is a command we ignore.
    if len(parts) < 4 or parts[1] != "vw":
        return None
    return PinUpdate(target=parts[0], pin=parts[2], value=parts[3])


def inflate_json(body: bytes) -> Any | None:
    """Decompress a zlib-compressed JSON frame body."""
    for decompress in (zlib.decompress, lambda b: zlib.decompress(b, 16 + zlib.MAX_WBITS)):
        try:
            raw = decompress(body)
        except zlib.error:
            continue
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
    return None
