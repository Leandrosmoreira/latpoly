"""Shared WebSocket reconnection and ping/pong logic."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import AsyncIterator, Optional

import websockets
import websockets.asyncio.client as ws_client

log = logging.getLogger(__name__)

# Backoff settings
INITIAL_BACKOFF = 0.5
MAX_BACKOFF = 30.0
BACKOFF_FACTOR = 2.0


async def ws_connect_forever(
    url: str,
    name: str,
    shutdown: asyncio.Event,
    ping_interval: float = 15.0,
    max_connection_age: float = 23 * 3600 + 50 * 60,  # 23h50m
    on_reconnect: Optional[asyncio.Event] = None,
) -> AsyncIterator[websockets.asyncio.client.ClientConnection]:
    """Yield a connected WebSocket, reconnecting with exponential backoff.

    Each iteration yields a fresh connection. The caller should iterate in a
    loop and handle messages from the yielded ws. When the inner loop breaks
    (connection error), the next iteration will reconnect.
    """
    backoff = INITIAL_BACKOFF
    reconnect_count = 0

    while not shutdown.is_set():
        connect_time = time.monotonic()
        try:
            async with ws_client.connect(
                url,
                ping_interval=ping_interval,
                ping_timeout=45.0,
                close_timeout=5.0,
                max_size=2**20,  # 1 MB
            ) as ws:
                reconnect_count += 1
                backoff = INITIAL_BACKOFF  # reset on success
                log.info("[%s] Connected (#%d) to %s", name, reconnect_count, url[:80])

                if on_reconnect:
                    on_reconnect.set()

                yield ws

                # If we get here, the caller's inner loop exited normally.
                # Check if we exceeded max connection age.
                age = time.monotonic() - connect_time
                if age >= max_connection_age:
                    log.info("[%s] Proactive reconnect after %.0fs", name, age)

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            OSError,
            asyncio.TimeoutError,
        ) as exc:
            if shutdown.is_set():
                break
            log.warning("[%s] Connection lost: %s. Reconnecting in %.1fs", name, exc, backoff)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=backoff)
                break  # shutdown during backoff
            except asyncio.TimeoutError:
                pass  # timeout expired, retry
            backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
