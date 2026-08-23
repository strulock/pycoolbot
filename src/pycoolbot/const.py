"""Protocol constants for the CoolBot Pro cloud service.

The CoolBot Pro app is an Ionic front end over Blynk. It talks to the server over
a WebSocket carrying binary Blynk frames:

    [cmd:1][msgId:2 big-endian][length:2 big-endian][body]

Body fields are NUL-separated. On a RESPONSE frame the length field carries a
status code instead of a body length.

Everything here was derived by observing the official web app and reading its
bundle; none of it is documented by the vendor, so treat it as version-sensitive.
"""

from __future__ import annotations

from typing import Final

WS_URL: Final = "wss://cbws.storeitcold.com/websocket"

# --- frame types ------------------------------------------------------------

CMD_RESPONSE: Final = 0
CMD_LOGIN: Final = 2
CMD_LOAD_PROFILE: Final = 4
CMD_PING: Final = 6
CMD_HARDWARE: Final = 20  # live pin push
CMD_LOAD_PROFILE_GZIPPED: Final = 24  # full account profile, zlib JSON
CMD_APP_SYNC: Final = 25  # request/replay of every pin value
CMD_DASH_GZIPPED: Final = 58  # single dashboard, zlib JSON
CMD_GRAPH_DATA: Final = 60  # historical series, packed binary

#: Frames whose body is zlib-compressed JSON.
COMPRESSED_CMDS: Final = frozenset({CMD_LOAD_PROFILE_GZIPPED, CMD_DASH_GZIPPED})

#: Status returned in a RESPONSE frame's length field on success.
STATUS_OK: Final = 200

#: Observed non-OK statuses. The server returns these in place of a body length.
STATUS_MESSAGES: Final = {
    2: "quota limit reached",
    3: "illegal command",
    4: "user already registered",
    5: "user not registered",
    6: "not allowed",
    7: "device not in network",
    8: "no active dashboard",
    9: "invalid token",
    10: "illegal command body",
}

# --- login ------------------------------------------------------------------
# The app sends: email \0 hash \0 "Other" \0 "12240000" \0 "Blynk"
# The last three identify the client. If the server ever enforces a minimum
# client version, APP_VERSION_CODE is what needs bumping.

APP_OS: Final = "Other"
APP_VERSION_CODE: Final = "12240000"
APP_NAME: Final = "Blynk"

# --- virtual pins -----------------------------------------------------------
# Each confirmed by cross-checking against the value the app displayed at the
# same moment. Temperatures are ALWAYS Fahrenheit on the wire; the app's F/C
# toggle is a local display preference and never changes what is sent.

PIN_ROOM_TEMP: Final = "0"
PIN_FINS_TEMP: Final = "1"
PIN_HARDWARE_STATUS: Final = "2"
PIN_SET_POINT: Final = "4"
PIN_NOTIFY_HIGH: Final = "12"
PIN_NOTIFY_LOW: Final = "16"
PIN_WIFI_DBM: Final = "18"
PIN_JUMPER_FIRMWARE: Final = "20"
PIN_MAC_ADDRESS: Final = "25"
PIN_JUMPER_HARDWARE: Final = "35"
PIN_COOLBOT_HARDWARE: Final = "36"

#: Pins that indicate a live measurement rather than a stored setting. Seeing one
#: of these arrive in a HARDWARE frame is what proves a reading is current.
LIVE_PINS: Final = frozenset({PIN_ROOM_TEMP, PIN_FINS_TEMP})

#: How often the device pushes temperatures, measured. Used to size timeouts.
PUSH_INTERVAL_SECONDS: Final = 15
