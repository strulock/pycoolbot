"""Tests for frame encoding/decoding and credential hashing."""

from __future__ import annotations

import base64
import gzip
import hashlib
import zlib

from pycoolbot.const import CMD_APP_SYNC, CMD_HARDWARE, CMD_LOGIN, CMD_RESPONSE
from pycoolbot.protocol import (
    Frame,
    decode_frames,
    encode_frame,
    hash_password,
    inflate_json,
    login_body,
    parse_pin_update,
    status_message,
)


class TestFrames:
    def test_roundtrip(self) -> None:
        buf = encode_frame(CMD_HARDWARE, 7, "123\0vw\x000\x0056.5")
        frames = decode_frames(buf)
        assert frames == [Frame(cmd=CMD_HARDWARE, msg_id=7, body=b"123\0vw\x000\x0056.5")]

    def test_multiple_frames_in_one_payload(self) -> None:
        buf = encode_frame(CMD_HARDWARE, 1, "a") + encode_frame(CMD_APP_SYNC, 2, "bb")
        frames = decode_frames(buf)
        assert [(f.cmd, f.msg_id, f.body) for f in frames] == [
            (CMD_HARDWARE, 1, b"a"),
            (CMD_APP_SYNC, 2, b"bb"),
        ]

    def test_response_frame_length_field_is_a_status(self) -> None:
        # A RESPONSE header carries a status code where the length would be,
        # and no body follows.
        buf = bytes([CMD_RESPONSE]) + (3).to_bytes(2, "big") + (200).to_bytes(2, "big")
        frames = decode_frames(buf)
        assert frames == [Frame(cmd=CMD_RESPONSE, msg_id=3, status=200)]

    def test_truncated_trailing_frame_is_dropped_not_guessed(self) -> None:
        whole = encode_frame(CMD_HARDWARE, 1, "complete")
        partial = encode_frame(CMD_HARDWARE, 2, "cut off here")[:-4]
        frames = decode_frames(whole + partial)
        assert len(frames) == 1
        assert frames[0].msg_id == 1

    def test_string_and_bytes_bodies_encode_identically(self) -> None:
        assert encode_frame(CMD_LOGIN, 1, "abc") == encode_frame(CMD_LOGIN, 1, b"abc")


class TestLogin:
    def test_hash_password_appends_raw_inner_digest(self) -> None:
        # The app hashes SHA256(password + SHA256(lowercased email)) with the
        # inner digest appended as raw bytes, not hex. Getting this wrong
        # produces a hash the server rejects.
        email, password = "User@Example.com", "hunter2"
        inner = hashlib.sha256(email.lower().encode()).digest()
        expected = base64.b64encode(hashlib.sha256(password.encode() + inner).digest())
        assert hash_password(email, password) == expected.decode("ascii")

    def test_email_case_does_not_change_the_hash(self) -> None:
        assert hash_password("A@B.C", "pw") == hash_password("a@b.c", "pw")

    def test_login_body_field_order(self) -> None:
        body = login_body("a@b.c", "pw")
        fields = body.decode().split("\0")
        assert fields[0] == "a@b.c"
        assert fields[1] == hash_password("a@b.c", "pw")
        assert len(fields) == 5


class TestPinUpdates:
    def test_parses_virtual_write(self) -> None:
        update = parse_pin_update(b"123-1\0vw\x000\x0056.570")
        assert update is not None
        assert (update.target, update.pin, update.value) == ("123-1", "0", "56.570")

    def test_ignores_non_virtual_writes(self) -> None:
        assert parse_pin_update(b"123\0dw\x001\x001") is None
        assert parse_pin_update(b"123\0vw\x000") is None
        assert parse_pin_update(b"") is None


class TestInflate:
    def test_zlib_payload(self) -> None:
        assert inflate_json(zlib.compress(b'{"id": 1}')) == {"id": 1}

    def test_gzip_payload(self) -> None:
        assert inflate_json(gzip.compress(b'[1, 2]')) == [1, 2]

    def test_garbage_returns_none(self) -> None:
        assert inflate_json(b"not compressed") is None
        assert inflate_json(zlib.compress(b"not json")) is None


class TestStatusMessages:
    def test_known_and_unknown_codes(self) -> None:
        assert status_message(5) == "user not registered"
        assert status_message(999) == "status 999"
