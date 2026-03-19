"""W1 — Binance combined stream worker (trade + bookTicker).

Multi-market: one worker per unique Binance symbol.
"""

from __future__ import annotations

import asyncio
import logging
import time

import orjson

from latpoly.config import Config
from latpoly.shared_state import BinanceState, SharedState
from latpoly.utils.ws_base import ws_connect_forever

log = logging.getLogger(__name__)


def _handle_trade(data: dict, bn: BinanceState) -> None:
    """Process a trade event and update state."""
    price = float(data["p"])
    qty = float(data["q"])
    trade_time_ms = data["T"]

    bn.last_trade_price = price
    bn.last_trade_qty = qty
    bn.last_trade_time_ms = trade_time_ms
    bn.ts_local_recv_ns = time.time_ns()
    bn.ts_mono_ns = time.monotonic_ns()
    bn.event_count += 1


def _handle_book_ticker(data: dict, bn: BinanceState) -> None:
    """Process a bookTicker event and update state."""
    bid = float(data["b"])
    ask = float(data["a"])
    bid_qty = float(data["B"])
    ask_qty = float(data["A"])

    bn.best_bid = bid
    bn.best_bid_qty = bid_qty
    bn.best_ask = ask
    bn.best_ask_qty = ask_qty
    bn.mid = (bid + ask) / 2.0
    bn.book_update_id = data.get("u", 0)
    bn.ts_local_recv_ns = time.time_ns()
    bn.ts_mono_ns = time.monotonic_ns()
    bn.event_count += 1


_HANDLERS_BY_EVENT = {
    "trade": _handle_trade,
    "bookTicker": _handle_book_ticker,
}

_HANDLERS_BY_STREAM = {
    "@trade": _handle_trade,
    "@bookTicker": _handle_book_ticker,
}


async def binance_worker(cfg: Config, state: SharedState, symbol: str) -> None:
    """Connect to Binance combined stream for a specific symbol."""
    url = cfg.binance_ws_url_for(symbol)
    bn = state.get_binance(symbol)
    log.info("Binance worker starting: %s → %s", symbol, url)

    async for ws in ws_connect_forever(
        url, f"binance-{symbol}", state.shutdown, ping_interval=15.0
    ):
        try:
            async for raw in ws:
                if state.shutdown.is_set():
                    break

                envelope = orjson.loads(raw)
                stream_name: str = envelope.get("stream", "")
                data: dict = envelope.get("data", {})

                event_type = data.get("e", "")
                handler = _HANDLERS_BY_EVENT.get(event_type)
                if handler is None:
                    for suffix, h in _HANDLERS_BY_STREAM.items():
                        if stream_name.endswith(suffix):
                            handler = h
                            break
                if handler:
                    handler(data, bn)

        except asyncio.CancelledError:
            log.info("Binance worker %s cancelled", symbol)
            return
        except Exception:
            if state.shutdown.is_set():
                return
            log.exception("Binance worker %s error, reconnecting", symbol)

    log.info("Binance worker %s stopped", symbol)
