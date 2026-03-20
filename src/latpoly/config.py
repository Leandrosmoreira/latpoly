"""Configuration with environment variable overrides."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


# ---------------------------------------------------------------------------
# Market slot definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MarketSlotDef:
    """Defines one Polymarket up/down market to monitor."""

    slot_id: str            # "btc-15m", "eth-5m", "sol-1h"
    binance_symbol: str     # "btcusdt", "ethusdt"
    coin: str               # "btc", "eth", "bitcoin", "solana"
    timeframe: str          # "5m", "15m", "1h", "4h"
    window_seconds: int     # 300, 900, 3600, 14400
    slug_pattern: str       # "timestamp" or "human_date"


ALL_KNOWN_SLOTS: tuple[MarketSlotDef, ...] = (
    MarketSlotDef("btc-5m",  "btcusdt", "btc",      "5m",   300,   "timestamp"),
    MarketSlotDef("btc-15m", "btcusdt", "btc",      "15m",  900,   "timestamp"),
    MarketSlotDef("btc-4h",  "btcusdt", "btc",      "4h",   14400, "timestamp"),
    MarketSlotDef("eth-5m",  "ethusdt", "eth",      "5m",   300,   "timestamp"),
    MarketSlotDef("eth-15m", "ethusdt", "eth",      "15m",  900,   "timestamp"),
    MarketSlotDef("eth-1h",  "ethusdt", "ethereum", "1h",   3600,  "human_date"),
    MarketSlotDef("btc-1h",  "btcusdt", "bitcoin",  "1h",   3600,  "human_date"),
    MarketSlotDef("sol-1h",  "solusdt", "solana",   "1h",   3600,  "human_date"),
    MarketSlotDef("xrp-1h",  "xrpusdt", "xrp",     "1h",   3600,  "human_date"),
)

# Default: only btc-15m (backward-compatible with single-market mode)
_DEFAULT_SLOT = (ALL_KNOWN_SLOTS[1],)  # btc-15m


def _parse_market_slots() -> tuple[MarketSlotDef, ...]:
    """Parse LATPOLY_MARKETS env var (JSON array) or return default."""
    raw = os.environ.get("LATPOLY_MARKETS", "")
    if not raw.strip():
        return _DEFAULT_SLOT
    try:
        items = json.loads(raw)
        return tuple(MarketSlotDef(**item) for item in items)
    except (json.JSONDecodeError, TypeError, KeyError) as exc:
        raise ValueError(f"Invalid LATPOLY_MARKETS JSON: {exc}") from exc


# ---------------------------------------------------------------------------
# Main config
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Config:
    # Binance
    binance_ws_base: str = field(
        default_factory=lambda: _env(
            "LATPOLY_BINANCE_WS", "wss://stream.binance.com:9443/stream"
        )
    )
    symbol: str = field(
        default_factory=lambda: _env("LATPOLY_SYMBOL", "BTCUSDT").lower()
    )

    # Market slots (multi-market)
    market_slots: tuple[MarketSlotDef, ...] = field(
        default_factory=_parse_market_slots
    )

    # Polymarket
    poly_ws_url: str = field(
        default_factory=lambda: _env(
            "LATPOLY_POLY_WS",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )
    )
    poly_rest_url: str = field(
        default_factory=lambda: _env(
            "LATPOLY_POLY_REST", "https://clob.polymarket.com"
        )
    )
    gamma_api_url: str = field(
        default_factory=lambda: _env(
            "LATPOLY_GAMMA_API", "https://gamma-api.polymarket.com"
        )
    )

    # Signal worker
    signal_interval: float = field(
        default_factory=lambda: _env_float("LATPOLY_SIGNAL_INTERVAL", 0.2)
    )
    queue_maxsize: int = field(
        default_factory=lambda: _env_int("LATPOLY_QUEUE_SIZE", 500)
    )

    # Discovery
    discovery_lead_s: float = field(
        default_factory=lambda: _env_float("LATPOLY_DISCOVERY_LEAD", 60.0)
    )

    # Writer
    writer_batch_size: int = field(
        default_factory=lambda: _env_int("LATPOLY_WRITER_BATCH", 50)
    )
    writer_batch_timeout: float = field(
        default_factory=lambda: _env_float("LATPOLY_WRITER_TIMEOUT", 1.0)
    )
    data_dir: str = field(
        default_factory=lambda: _env("LATPOLY_DATA_DIR", "./data")
    )

    # Health
    health_interval: float = field(
        default_factory=lambda: _env_float("LATPOLY_HEALTH_INTERVAL", 10.0)
    )

    # Trading mode: "paper" (default, no real orders) or "live" (real orders)
    trading_mode: str = field(
        default_factory=lambda: _env("LATPOLY_TRADING_MODE", "paper")
    )

    # Logging
    log_level: str = field(
        default_factory=lambda: _env("LATPOLY_LOG_LEVEL", "INFO")
    )

    # Derived
    @property
    def binance_symbols(self) -> set[str]:
        """Unique Binance symbols needed across all market slots."""
        return {slot.binance_symbol for slot in self.market_slots}

    @property
    def binance_ws_url(self) -> str:
        """Backward-compat: single-symbol WS URL (uses self.symbol)."""
        sym = self.symbol
        return f"{self.binance_ws_base}?streams={sym}@trade/{sym}@bookTicker"

    def binance_ws_url_for(self, symbol: str) -> str:
        """Build Binance WS URL for a specific symbol."""
        return f"{self.binance_ws_base}?streams={symbol}@trade/{symbol}@bookTicker"
