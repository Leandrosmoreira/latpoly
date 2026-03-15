"""W1 — Binance combined stream worker (trade + bookTicker)."""

from __future__ import annotations

import asyncio
import logging
import time

import orjson

from latpoly.config import Config
from latpoly.shared_state import SharedState
from latpoly.utils.ws_base import ws_connect_forever

log = logging.getLogger(__name__)


def _handle_trade(data: dict, state: SharedState) -> None:
    """Process a trade event and update shared state."""
    price = float(data["p"])
    qty = float(data["q"])
    trade_time_ms = data["T"]

    bn = state.binance
    bn.last_trade_price = price
    bn.last_trade_qty = qty
    bn.last_trade_time_ms = trade_time_ms
    bn.ts_local_recv_ns = time.time_ns()
    bn.ts_mono_ns = time.monotonic_ns()
    bn.event_count += 1


def _handle_book_ticker(data: dict, state: SharedState) -> None:
    """Process a bookTicker event and update shared state."""
    bid = float(data["b"])
    ask = float(data["a"])
    bid_qty = float(data["B"])
    ask_qty = float(data["A"])

    bn = state.binance
    bn.best_bid = bid
    bn.best_bid_qty = bid_qty
    bn.best_ask = ask
    bn.best_ask_qty = ask_qty
    bn.mid = (bid + ask) / 2.0
    bn.book_update_id = data.get("u", 0)
    bn.ts_local_recv_ns = time.time_ns()
    bn.ts_mono_ns = time.monotonic_ns()
    bn.event_count += 1


_HANDLERS = {
    "trade": _handle_trade,
    "bookTicker": _handle_book_ticker,
}


async def binance_worker(cfg: Config, state: SharedState) -> None:
    """Connect to Binance combined stream and update SharedState."""
    url = cfg.binance_ws_url
    log.info("Binance worker starting: %s", url)

    async for ws in ws_connect_forever(
        url, "binance", state.shutdown, ping_interval=15.0
    ):
        try:
            async for raw in ws:
                if state.shutdown.is_set():
                    break

                envelope = orjson.loads(raw)
                stream_name: str = envelope.get("stream", "")
                data: dict = envelope.get("data", {})

                # Dispatch: stream name ends with @trade or @bookTicker
                event_type = data.get("e", "")
                handler = _HANDLERS.get(event_type)
                if handler:
                    handler(data, state)

        except asyncio.CancelledError:
            log.info("Binance worker cancelled")
            return
        except Exception:
            if state.shutdown.is_set():
                return
            log.exception("Binance worker error, reconnecting")

    log.info("Binance worker stopped")
