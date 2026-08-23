# pycoolbot

Async, read-only Python client for the [CoolBot Pro](https://storeitcold.com/) walk-in
cooler cloud service. It speaks the app's own Blynk WebSocket protocol directly — no
browser, no vendor SDK.

This is the protocol library behind the
[ha-coolbot](https://github.com/strulock/ha-coolbot) Home Assistant integration, split
out so anything can use it.

```python
import asyncio
from pycoolbot import CoolbotClient

async def main() -> None:
    async with CoolbotClient("you@example.com", "password") as client:
        for device in await client.async_get_devices():
            print(device.name, device.room_temp_f, "°F")

asyncio.run(main())
```

## Read-only by design

This library never writes to a device. Nothing here can change a set point, and the
test suite enforces that the client only ever sends login, profile, sync, and ping
frames.

## What you get

`CoolbotClient.async_get_devices()` returns a list of `CoolbotDevice` records with:

- `room_temp_f`, `fins_temp_f`, `set_point_f` — temperatures, always Fahrenheit on the wire
- `notify_low_f`, `notify_high_f` — alert thresholds
- `wifi_dbm`, `hardware_status`, `mac_address`, firmware/hardware versions
- `available`, `is_provisioned`, `last_data_at`, `data_age_seconds` — freshness signals

By default `async_get_devices()` waits for a **live** temperature push before
returning, because the server replays a cached snapshot on connect that can be minutes
out of date. `data_age_seconds` is `None` when no live push has arrived.

Device enumeration is driven by the account profile, not by which data streams appear:
the server keeps stale pin state on empty device slots, and trusting streams would
invent phantom devices. Unprovisioned slots report all readings as `None`.

## Install

```bash
pip install pycoolbot
```

Requires Python 3.11+. The only dependency is `aiohttp`.

## Caveats

The protocol was derived by observing the official web app and reading its bundle.
None of it is documented by the vendor (Store It Cold, LLC), so treat it as
version-sensitive. This project is not affiliated with or endorsed by Store It Cold.

## License

[MIT](LICENSE)
