"""Request/response plumbing: registration order and socket failure contract."""

from __future__ import annotations

import asyncio
import gc

import aiohttp
import pytest

from pycoolbot import CoolbotClient, CoolbotConnectionError, CoolbotError
from pycoolbot.const import CMD_PING, CMD_RESPONSE, STATUS_OK
from pycoolbot.protocol import Frame, decode_frames


class _RespondingSocket:
    """Answers during the send, before the caller resumes.

    This is the real ordering: ``send_bytes`` yields to the event loop, so the
    reader task can decode the response before ``_request`` continues.
    """

    closed = False

    def __init__(self, client: CoolbotClient) -> None:
        self._client = client

    async def send_bytes(self, data: bytes) -> None:
        await asyncio.sleep(0)
        for frame in decode_frames(data):
            self._client._handle(
                Frame(cmd=CMD_RESPONSE, msg_id=frame.msg_id, status=STATUS_OK)
            )


class _SilentSocket:
    """Accepts frames and never answers."""

    closed = False

    async def send_bytes(self, data: bytes) -> None:
        return None


class _FailingSocket:
    """A socket whose writer fails the way a dropped connection does."""

    closed = False

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def send_bytes(self, data: bytes) -> None:
        raise self._error


class _DroppingSocket:
    """Loses the connection while the send is suspended.

    The reader notices first and fails every pending response; only then does
    the writer report its own failure.
    """

    closed = False

    def __init__(self, client: CoolbotClient) -> None:
        self._client = client

    async def send_bytes(self, data: bytes) -> None:
        await asyncio.sleep(0)
        self._client._fail_pending()
        raise ConnectionResetError("connection reset by peer")


def _client() -> CoolbotClient:
    # A short timeout so a regression fails fast instead of hanging.
    return CoolbotClient("user@example.com", "hunter2", request_timeout=0.5)


def test_a_response_arriving_during_the_send_is_not_missed() -> None:
    """The pending response must be registered before the frame goes out.

    Registering afterwards loses the race for fast commands: the response is
    discarded as unknown and the caller waits out the full request timeout.
    """

    async def scenario() -> int:
        client = _client()
        client._ws = _RespondingSocket(client)
        return await client._request(CMD_PING)

    assert asyncio.run(scenario()) == STATUS_OK


def test_an_unanswered_request_cleans_up_after_itself() -> None:
    """A timed-out request leaves no pending future behind."""

    async def scenario() -> dict[int, asyncio.Future[int]]:
        client = _client()
        client._ws = _SilentSocket()
        with pytest.raises(CoolbotConnectionError):
            await client._request(CMD_PING)
        return client._responses

    assert asyncio.run(scenario()) == {}


@pytest.mark.parametrize(
    "error",
    [
        aiohttp.ClientError("writer is gone"),
        ConnectionResetError("connection reset by peer"),
    ],
)
def test_send_failures_are_presented_as_our_own_error(error: BaseException) -> None:
    """Callers handle CoolbotError; a raw aiohttp error would escape them.

    Letting one through skips the caller's reconnect and cleanup paths, so it
    surfaces as an unexpected error instead of a retry, with the client still
    open.
    """

    async def scenario() -> None:
        client = _client()
        client._ws = _FailingSocket(error)

        with pytest.raises(CoolbotConnectionError) as caught:
            await client._request(CMD_PING)
        assert isinstance(caught.value, CoolbotError)
        assert not client._responses

        # The same contract covers fire-and-forget sends, which is how the
        # dashboard subscription is issued.
        with pytest.raises(CoolbotConnectionError):
            await client._send(CMD_PING)

    asyncio.run(scenario())


def test_a_drop_during_the_send_leaves_no_unretrieved_exception() -> None:
    """Both halves of a dropped connection are accounted for.

    The reader fails the pending response while the send is suspended, then the
    writer raises. Whichever error surfaces, the future must not be discarded
    with an exception still sitting in it, or asyncio reports it as never
    retrieved when the future is collected.
    """
    unretrieved: list[str] = []

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(
            lambda _loop, context: unretrieved.append(context["message"])
        )
        client = _client()
        client._ws = _DroppingSocket(client)

        raised = False
        try:
            await client._request(CMD_PING)
        except CoolbotConnectionError:
            # Deliberately not kept: the traceback references the frame holding
            # the future, which would keep it alive past the collection below.
            raised = True
        assert raised
        assert not client._responses

        # The report happens when the future is finalized, so collect while the
        # loop is still open and able to hand it to the handler.
        gc.collect()

    asyncio.run(scenario())
    assert not [message for message in unretrieved if "never retrieved" in message]


def test_sending_without_a_socket_is_a_connection_error() -> None:
    """Nothing may be sent before connecting."""

    async def scenario() -> None:
        with pytest.raises(CoolbotConnectionError):
            await _client()._send(CMD_PING)

    asyncio.run(scenario())


class _UncloseableSocket:
    """A socket whose close itself fails."""

    closed = False

    async def close(self) -> None:
        raise aiohttp.ClientError("close failed")


class _UncloseableSession:
    """A session whose close itself fails."""

    async def close(self) -> None:
        raise RuntimeError("session close failed")


def test_close_never_raises() -> None:
    """Closing is best-effort cleanup and must not raise.

    It usually runs while some other failure is being handled - a rejected
    login, a lost connection - and an error escaping here would replace that
    failure with cleanup noise.
    """

    async def scenario() -> None:
        client = _client()
        client._ws = _UncloseableSocket()
        client._session = _UncloseableSession()

        await client.async_close()

        assert client._ws is None
        assert client._session is None

    asyncio.run(scenario())
