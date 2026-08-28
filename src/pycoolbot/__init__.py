"""Read-only client for the CoolBot Pro cloud service.

Speaks the app's own Blynk WebSocket protocol directly — no browser required.

    async with CoolbotClient(email, password) as client:
        for device in await client.async_get_devices():
            print(device.name, device.room_temp_f)

This library never writes to a device. Nothing here can change a set point.
"""

from __future__ import annotations

from .client import (
    CoolbotAuthError,
    CoolbotClient,
    CoolbotConnectionError,
    CoolbotError,
)
from .models import CoolbotDevice, build_devices, target_for
from .protocol import hash_password

__version__ = "0.1.5"

__all__ = [
    "CoolbotAuthError",
    "CoolbotClient",
    "CoolbotConnectionError",
    "CoolbotDevice",
    "CoolbotError",
    "build_devices",
    "hash_password",
    "target_for",
]
