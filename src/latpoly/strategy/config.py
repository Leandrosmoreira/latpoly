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
        default_factory=lambda: _env_float("LATPOLY_STRAT_ENTRY_MAX_S", 900.0)  # sweep: full 15min window
    )
    entry_window_min_s: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_ENTRY_MIN_S", 30.0)
    )

    # --- Entry thresholds ---
    zscore_entry_threshold: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_ZSCORE_THRESHOLD", 2.0)
    )
    min_ret_1s_confirm: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_RET1S", 0.0)  # sweep: irrelevant
    )

    # --- Spread / liquidity ---
    max_spread_entry: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_SPREAD", 0.08)
    )
    min_depth_contracts: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_DEPTH", 100.0)
    )

    # --- Position sizing ---
    # Polymarket minimum: 5 shares for maker orders
    # Below 5 shares → forced taker exit (1% fee)
    base_size_contracts: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_BASE_SIZE", 5)
    )
    max_position_contracts: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MAX_POSITION", 5)
    )
    # Multi-position: up to N concurrent positions, capped by exposure
    max_concurrent_positions: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MAX_CONCURRENT", 10)
    )
    max_exposure_frac: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_EXPOSURE", 0.50)  # 50% of bankroll
    )
    # Minimum shares for maker exit on Polymarket
    min_maker_size: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MIN_MAKER_SIZE", 5)
    )

    # --- Probability range filter ---
    # Only enter when the contract mid is in a responsive zone.
    # At extremes (< 0.10 or > 0.90) probability barely reacts to BTC moves.
    min_mid_entry: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_MID", 0.15)
    )
    max_mid_entry: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_MID", 0.70)  # sweep: 0.70 optimal
    )

    # --- BTC-to-PM rate (calibrate with calibrate_rate.py!) ---
    btc_to_pm_base_rate: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_BTC_PM_RATE", 0.002)  # sweep: 0.002 optimal
    )
    min_bn_move_abs: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MIN_BN_MOVE", 6.0)  # sweep: $6 min
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

    # --- Entry mode ---
    # "maker": limit order at best_bid (0% fee, better price, risk of no fill)
    # "taker": market order at best_ask (pays taker fee, guaranteed fill)
    entry_as_maker: bool = field(
        default_factory=lambda: _env_bool("LATPOLY_STRAT_ENTRY_MAKER", True)
    )

    # --- Cost model ---
    # Polymarket crypto fee formula: fee = size * price * fee_rate * (price * (1 - price))^fee_exponent
    # Max effective rate = 1.56% at price=0.50
    # Maker: 0% fee (+ 20% rebate from taker fees, ignored in backtest)
    taker_fee_rate: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_TAKER_FEE_RATE", 0.25)
    )
    taker_fee_exponent: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_TAKER_FEE_EXP", 2.0)
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
    # Stop-loss: max loss per contract before forced exit (taker)
    # $0.03 × 100 contracts = $3.00 max loss per trade
    stop_loss_per_contract: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_STOP_LOSS", 0.03)
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
    initial_bankroll: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_BANKROLL", 100.0)
    )
    max_daily_loss: float = field(
        default_factory=lambda: _env_float("LATPOLY_STRAT_MAX_DAILY_LOSS", 50.0)  # 50% of $100 bankroll
    )
    max_daily_trades: int = field(
        default_factory=lambda: _env_int("LATPOLY_STRAT_MAX_DAILY_TRADES", 999_999)  # effectively unlimited
    )
