"""Shared state between workers — latest-value overwrite pattern."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BinanceState:
    """Latest Binance spot data. Written by W1, read by W3."""

    last_trade_price: Optional[float] = None
    last_trade_qty: Optional[float] = None
    last_trade_time_ms: int = 0
    best_bid: Optional[float] = None
    best_bid_qty: Optional[float] = None
    best_ask: Optional[float] = None
    best_ask_qty: Optional[float] = None
    mid: Optional[float] = None
    book_update_id: int = 0

    # Timestamps (set by collector on each message)
    ts_local_recv_ns: int = 0
    ts_mono_ns: int = 0

    # Health counters
    event_count: int = 0
    reconnect_count: int = 0


@dataclass
class MarketInfo:
    """Discovered Polymarket BTC 15m market."""

    condition_id: str = ""
    slug: str = ""
    yes_token_id: str = ""
    no_token_id: str = ""
    end_ts_s: float = 0.0  # epoch seconds when market expires
    strike: float = 0.0
    question: str = ""


@dataclass
class PolymarketState:
    """Latest Polymarket data. Written by W2, read by W3."""

    market: MarketInfo = field(default_factory=MarketInfo)

    yes_best_bid: Optional[float] = None
    yes_best_ask: Optional[float] = None
    no_best_bid: Optional[float] = None
    no_best_ask: Optional[float] = None
    mid_yes: Optional[float] = None
    mid_no: Optional[float] = None
    spread_yes: Optional[float] = None
    spread_no: Optional[float] = None
    last_trade_price_yes: Optional[float] = None
    last_trade_price_no: Optional[float] = None

    # Timestamps
    ts_local_recv_ns: int = 0
    ts_mono_ns: int = 0

    # Health counters
    event_count: int = 0
    reconnect_count: int = 0


@dataclass
class SharedState:
    """Central state shared across all workers."""

    binance: BinanceState = field(default_factory=BinanceState)
    polymarket: PolymarketState = field(default_factory=PolymarketState)

    # Shutdown signal
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)

    # Signal worker tick counter
    signal_tick_count: int = 0
    writer_records_written: int = 0
    writer_bytes_written: int = 0

    @property
    def binance_ready(self) -> bool:
        return self.binance.best_bid is not None

    @property
    def polymarket_ready(self) -> bool:
        return self.polymarket.yes_best_bid is not None

    @property
    def ready(self) -> bool:
        return self.binance_ready and self.polymarket_ready
