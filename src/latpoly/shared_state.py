"""Shared state between workers — latest-value overwrite pattern.

Multi-market: state is organized by slot_id (Polymarket) and symbol (Binance).
"""

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
    """Discovered Polymarket up/down market."""

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

    # Book depth: list of (price, size) tuples, best first (max 10 levels)
    yes_bids_levels: list = field(default_factory=list)
    yes_asks_levels: list = field(default_factory=list)
    no_bids_levels: list = field(default_factory=list)
    no_asks_levels: list = field(default_factory=list)

    # Timestamps
    ts_local_recv_ns: int = 0
    ts_mono_ns: int = 0

    # Health counters
    event_count: int = 0
    reconnect_count: int = 0


@dataclass
class SharedState:
    """Central state shared across all workers.

    Multi-market: binance_states keyed by symbol, polymarket_states keyed by slot_id.
    Backward-compat: .binance and .polymarket properties return first/default entries.
    """

    # Multi-market state dicts
    binance_states: dict[str, BinanceState] = field(default_factory=dict)
    polymarket_states: dict[str, PolymarketState] = field(default_factory=dict)

    # Shutdown signal
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)

    # Global counters
    signal_tick_count: int = 0
    writer_records_written: int = 0
    writer_bytes_written: int = 0

    def get_binance(self, symbol: str) -> BinanceState:
        """Get or create BinanceState for a symbol."""
        if symbol not in self.binance_states:
            self.binance_states[symbol] = BinanceState()
        return self.binance_states[symbol]

    def get_polymarket(self, slot_id: str) -> PolymarketState:
        """Get or create PolymarketState for a slot."""
        if slot_id not in self.polymarket_states:
            self.polymarket_states[slot_id] = PolymarketState()
        return self.polymarket_states[slot_id]

    def slot_ready(self, slot_id: str, binance_symbol: str) -> bool:
        """Check if a specific slot has data from both sources."""
        bn = self.binance_states.get(binance_symbol)
        pm = self.polymarket_states.get(slot_id)
        if bn is None or pm is None:
            return False
        return bn.best_bid is not None and pm.yes_best_bid is not None

    # --- Backward-compat properties (used by single-market code paths) ---

    @property
    def binance(self) -> BinanceState:
        """Default BinanceState (first entry or empty)."""
        if self.binance_states:
            return next(iter(self.binance_states.values()))
        # Create a default one
        return self.get_binance("btcusdt")

    @property
    def polymarket(self) -> PolymarketState:
        """Default PolymarketState (first entry or empty)."""
        if self.polymarket_states:
            return next(iter(self.polymarket_states.values()))
        return self.get_polymarket("btc-15m")

    @property
    def binance_ready(self) -> bool:
        return any(bn.best_bid is not None for bn in self.binance_states.values())

    @property
    def polymarket_ready(self) -> bool:
        return any(pm.yes_best_bid is not None for pm in self.polymarket_states.values())

    @property
    def ready(self) -> bool:
        """True if at least one slot has both sources ready."""
        # At least one polymarket slot has data AND at least one binance feed has data
        has_pm = any(pm.yes_best_bid is not None for pm in self.polymarket_states.values())
        has_bn = any(bn.best_bid is not None for bn in self.binance_states.values())
        return has_pm and has_bn
