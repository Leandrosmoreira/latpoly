"""Strategy configuration with environment variable overrides.

All thresholds are configurable via env vars prefixed with LATPOLY_STRAT_.
Follows the same frozen-dataclass pattern as the main Config.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    return os.environ.get(key, str(default)).lower() in ("true", "1", "yes")


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """All strategy parameters — tunable via environment variables."""

    # --- Entry window (seconds before expiry) ---
    entry_window_max_s: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_ENTRY_MAX_S", 300.0)
    )
    entry_window_min_s: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_ENTRY_MIN_S", 30.0)
    )

    # --- Entry thresholds ---
    zscore_entry_threshold: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_ZSCORE_THRESHOLD", 2.0)
    )
    min_ret_1s_confirm: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_RET1S", 0.5)
    )

    # --- Spread / liquidity ---
    max_spread_entry: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_SPREAD", 0.08)
    )
    min_depth_contracts: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_DEPTH", 100.0)
    )

    # --- Position sizing ---
    base_size_contracts: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_BASE_SIZE", 100)
    )
    max_position_contracts: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MAX_POSITION", 500)
    )

    # --- Risk filters ---
    min_distance_to_strike: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_DIST_STRIKE", 5.0)
    )
    max_data_age_ms: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_AGE_MS", 2000.0)
    )
    cooldown_ticks: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_COOLDOWN", 10)
    )
    min_net_edge: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_NET_EDGE", 0.005)
    )

    # --- Cost model ---
    taker_fee_rate: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_TAKER_FEE", 0.01)
    )
    maker_fee_rate: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAKER_FEE", 0.0)
    )

    # --- Exit strategy ---
    exit_profit_fraction: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_EXIT_FRAC", 0.5)
    )
    max_hold_ticks: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MAX_HOLD", 50)
    )
    hold_to_expiry_distance: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_HOLD_EXPIRY_DIST", 50.0)
    )

    # --- Time weight (aggression near expiry) ---
    time_weight_min: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_TW_MIN", 1.0)
    )
    time_weight_max: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_TW_MAX", 2.0)
    )

    # --- Kill switches ---
    max_daily_loss: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_DAILY_LOSS", 50.0)
    )
    max_daily_trades: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MAX_DAILY_TRADES", 200)
    )
