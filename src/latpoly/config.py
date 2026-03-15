"""Configuration with environment variable overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


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

    # Logging
    log_level: str = field(
        default_factory=lambda: _env("LATPOLY_LOG_LEVEL", "INFO")
    )

    # Derived
    @property
    def binance_ws_url(self) -> str:
        sym = self.symbol
        return f"{self.binance_ws_base}?streams={sym}@trade/{sym}@bookTicker"
