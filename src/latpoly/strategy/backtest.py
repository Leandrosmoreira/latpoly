#!/usr/bin/env python3
"""Backtester for the latpoly latency arbitrage strategy.

Reads JSONL tick files, runs StrategyEngine over them, tracks portfolio state,
and produces a detailed BacktestResult including equity curve, drawdown, and
per-trade breakdown.

Usage:
    python -m latpoly.strategy.backtest data/daily/*.jsonl
    python -m latpoly.strategy.backtest data/daily/*.jsonl --sweep
    LATPOLY_STRAT_ZSCORE_THRESHOLD=2.5 python -m latpoly.strategy.backtest data/daily/*.jsonl
"""

from __future__ import annotations

import itertools
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from latpoly.analysis.lag_report import load_ticks
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import Signal, StrategyEngine


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Trade:
    """One completed trade."""

    condition_id: str
    side: str           # "YES" or "NO"
    entry_price: float
    exit_price: float
    size: int
    taker_fee: float    # entry fee paid
    pnl_net: float
    exit_type: str      # MAKER, TIMEOUT, REVERSAL, EXPIRY_WIN, EXPIRY_LOSS
    hold_ticks: int
    entry_tick_idx: int
    exit_tick_idx: int
    entry_tte_ms: Optional[float] = None
    time_bucket: Optional[str] = None


@dataclass
class BacktestResult:
    """Summary of a full backtest run."""

    total_ticks: int
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    avg_pnl_per_trade: float
    max_drawdown: float
    sharpe: float
    total_fees: float
    trades: list[Trade]
    equity_curve: list[float]
    params: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Portfolio tracker
# ---------------------------------------------------------------------------


class Portfolio:
    """Tracks virtual cash, positions, P&L, and equity curve."""

    def __init__(self, initial_cash: float = 1000.0) -> None:
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.trades: list[Trade] = []
        self.equity_curve: list[float] = [initial_cash]
        self._peak_equity = initial_cash
        self._max_drawdown = 0.0

    def record_entry(self, signal: Signal, tick: dict) -> None:
        """Debit cash for an entry. Actual trade recording happens on exit."""
        # Cost = entry_price * size * (1 + taker_fee)
        if signal.entry_price is None:
            return
        cost = signal.entry_price * signal.size * (1.0 + 0.01)  # taker fee
        self.cash -= cost

    def record_exit(self, engine_trade: dict, ticks: list[dict]) -> None:
        """Record a completed trade from the engine's closed_trades log."""
        entry_idx = engine_trade["entry_tick_idx"]
        exit_idx = engine_trade["exit_tick_idx"]
        entry_price = engine_trade["entry_price"]
        exit_price = engine_trade["exit_price"]
        size = engine_trade["size"]
        exit_type = engine_trade["exit_type"]
        pnl_net = engine_trade["pnl_net"]

        # Compute entry fee
        taker_fee = entry_price * size * 0.01

        # Compute exit fee (depends on type)
        if exit_type in ("EXPIRY_WIN", "EXPIRY_LOSS"):
            exit_fee = 0.0
        elif exit_type == "MAKER":
            exit_fee = 0.0  # maker fee = 0%
        else:
            exit_fee = exit_price * size * 0.01  # taker exit

        # Credit cash for exit
        if exit_type in ("EXPIRY_WIN", "EXPIRY_LOSS"):
            revenue = exit_price * size
        elif exit_type == "MAKER":
            revenue = exit_price * size
        else:
            revenue = exit_price * size * (1.0 - 0.01)
        self.cash += revenue

        # Lookup tick data for metadata
        entry_tte_ms = None
        time_bucket = None
        if 0 <= entry_idx < len(ticks):
            entry_tick = ticks[entry_idx]
            entry_tte_ms = entry_tick.get("time_to_expiry_ms")
            time_bucket = entry_tick.get("time_bucket")

        trade = Trade(
            condition_id=engine_trade["condition_id"],
            side=engine_trade["side"],
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            taker_fee=taker_fee,
            pnl_net=pnl_net,
            exit_type=exit_type,
            hold_ticks=engine_trade["hold_ticks"],
            entry_tick_idx=entry_idx,
            exit_tick_idx=exit_idx,
            entry_tte_ms=entry_tte_ms,
            time_bucket=time_bucket,
        )
        self.trades.append(trade)

        # Update equity tracking
        equity = self.cash
        self.equity_curve.append(equity)
        if equity > self._peak_equity:
            self._peak_equity = equity
        dd = self._peak_equity - equity
        if dd > self._max_drawdown:
            self._max_drawdown = dd

    def summarize(self, total_ticks: int, params: dict | None = None) -> BacktestResult:
        """Produce a BacktestResult from all recorded trades."""
        n = len(self.trades)
        wins = sum(1 for t in self.trades if t.pnl_net > 0)
        losses = n - wins
        total_pnl = sum(t.pnl_net for t in self.trades)
        avg_pnl = total_pnl / n if n > 0 else 0.0
        total_fees = sum(t.taker_fee for t in self.trades)
        win_rate = wins / n if n > 0 else 0.0

        # Sharpe ratio from per-trade P&L
        sharpe = 0.0
        if n > 1:
            pnls = [t.pnl_net for t in self.trades]
            mean = sum(pnls) / n
            variance = sum((p - mean) ** 2 for p in pnls) / (n - 1)
            std = math.sqrt(variance) if variance > 0 else 0.0
            sharpe = (mean / std) * math.sqrt(n) if std > 0 else 0.0

        return BacktestResult(
            total_ticks=total_ticks,
            total_trades=n,
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            total_pnl=round(total_pnl, 4),
            avg_pnl_per_trade=round(avg_pnl, 4),
            max_drawdown=round(self._max_drawdown, 4),
            sharpe=round(sharpe, 4),
            total_fees=round(total_fees, 4),
            trades=self.trades,
            equity_curve=self.equity_curve,
            params=params or {},
        )


# ---------------------------------------------------------------------------
# Core backtest runner
# ---------------------------------------------------------------------------


def run_backtest(
    ticks: list[dict],
    cfg: StrategyConfig | None = None,
    initial_cash: float = 1000.0,
    params: dict | None = None,
) -> BacktestResult:
    """Run a full backtest over a list of normalized ticks.

    Args:
        ticks: List of tick dicts from JSONL files.
        cfg: Strategy config. If None, uses defaults (with env overrides).
        initial_cash: Starting virtual cash.
        params: Optional dict of param overrides (for sweep labeling).

    Returns:
        BacktestResult with all trades and equity curve.
    """
    if cfg is None:
        cfg = StrategyConfig()

    engine = StrategyEngine(cfg)
    portfolio = Portfolio(initial_cash=initial_cash)

    last_processed_trade_idx = 0

    for idx, tick in enumerate(ticks):
        signal = engine.on_tick(tick, idx)

        # On entry: debit cash
        if signal.action in ("BUY_YES", "BUY_NO"):
            portfolio.record_entry(signal, tick)

        # Check for newly completed trades from engine
        while last_processed_trade_idx < len(engine.closed_trades):
            trade_data = engine.closed_trades[last_processed_trade_idx]
            portfolio.record_exit(trade_data, ticks)
            last_processed_trade_idx += 1

    return portfolio.summarize(len(ticks), params=params)


# ---------------------------------------------------------------------------
# Parameter sweep
# ---------------------------------------------------------------------------

# Default parameter grid for sweep
DEFAULT_PARAM_GRID: dict[str, list] = {
    "zscore_entry_threshold": [1.5, 2.0, 2.5, 3.0],
    "entry_window_min_s": [15.0, 30.0, 45.0],
    "min_distance_to_strike": [3.0, 5.0, 10.0],
    "exit_profit_fraction": [0.3, 0.5, 0.7],
}


def _make_config_with_overrides(overrides: dict) -> StrategyConfig:
    """Create a StrategyConfig with specific parameter overrides.

    Since StrategyConfig is frozen with field defaults from env vars,
    we create a default instance and then use object.__setattr__ to override.
    """
    cfg = StrategyConfig()
    for k, v in overrides.items():
        object.__setattr__(cfg, k, v)
    return cfg


def run_sweep(
    ticks: list[dict],
    param_grid: dict[str, list] | None = None,
    initial_cash: float = 1000.0,
    top_n: int = 10,
) -> list[BacktestResult]:
    """Run parameter sweep over all combinations in param_grid.

    Args:
        ticks: Tick data.
        param_grid: Dict of param_name -> list of values. If None, uses default grid.
        initial_cash: Starting cash for each run.
        top_n: Return top N results by Sharpe ratio.

    Returns:
        List of BacktestResult, sorted by Sharpe ratio descending.
    """
    if param_grid is None:
        param_grid = DEFAULT_PARAM_GRID

    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())

    combos = list(itertools.product(*param_values))
    total = len(combos)
    print(f"\n  Running parameter sweep: {total} combinations...")

    results: list[BacktestResult] = []

    for i, combo in enumerate(combos, 1):
        overrides = dict(zip(param_names, combo))

        if i % 10 == 0 or i == 1:
            print(f"    [{i}/{total}] {overrides}")

        cfg = _make_config_with_overrides(overrides)
        result = run_backtest(ticks, cfg=cfg, initial_cash=initial_cash, params=overrides)
        results.append(result)

    # Sort by Sharpe ratio descending
    results.sort(key=lambda r: r.sharpe, reverse=True)
    return results[:top_n]


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------


def print_backtest_report(result: BacktestResult) -> None:
    """Print a formatted backtest report to stdout."""
    print(f"\n{'='*70}")
    print(f"  LATPOLY BACKTEST REPORT")
    print(f"  Ticks: {result.total_ticks:,}")
    if result.params:
        params_str = "  ".join(f"{k}={v}" for k, v in result.params.items())
        print(f"  Params: {params_str}")
    print(f"{'='*70}\n")

    if result.total_trades == 0:
        print("  No trades generated. Check thresholds or data.\n")
        print(f"{'='*70}\n")
        return

    print(f"  Total trades:      {result.total_trades}")
    print(f"  Win rate:          {result.win_rate*100:.1f}% ({result.wins}W / {result.losses}L)")
    print(f"  Total P&L:         ${result.total_pnl:.2f}")
    print(f"  Avg P&L/trade:     ${result.avg_pnl_per_trade:.4f}")
    print(f"  Total fees:        ${result.total_fees:.2f}")
    print(f"  Max drawdown:      ${result.max_drawdown:.2f}")
    print(f"  Sharpe ratio:      {result.sharpe:.4f}")
    print()

    # --- By Exit Type ---
    by_exit: dict[str, list[Trade]] = defaultdict(list)
    for t in result.trades:
        by_exit[t.exit_type].append(t)

    print("  --- By Exit Type ---")
    for et in ["MAKER", "TIMEOUT", "REVERSAL", "EXPIRY_WIN", "EXPIRY_LOSS"]:
        trades = by_exit.get(et, [])
        if not trades:
            continue
        avg_pnl = sum(t.pnl_net for t in trades) / len(trades)
        win_n = sum(1 for t in trades if t.pnl_net > 0)
        print(f"  {et:15s}  {len(trades):4d} trades  avg=${avg_pnl:+.4f}  win={win_n}/{len(trades)}")
    print()

    # --- By Time Bucket ---
    by_bucket: dict[str, list[Trade]] = defaultdict(list)
    for t in result.trades:
        bucket = t.time_bucket or "unknown"
        by_bucket[bucket].append(t)

    print("  --- By Time Bucket ---")
    bucket_order = [">600s", "600-300s", "300-120s", "120-60s", "<60s", "unknown"]
    for bname in bucket_order:
        trades = by_bucket.get(bname, [])
        if not trades:
            continue
        avg_pnl = sum(t.pnl_net for t in trades) / len(trades)
        win_rate = sum(1 for t in trades if t.pnl_net > 0) / len(trades)
        print(f"  {bname:10s}  {len(trades):4d} trades  win={win_rate*100:.0f}%  avg=${avg_pnl:+.4f}")
    print()

    # --- By Side ---
    print("  --- By Side ---")
    for side in ["YES", "NO"]:
        trades = [t for t in result.trades if t.side == side]
        if not trades:
            continue
        total = sum(t.pnl_net for t in trades)
        avg = total / len(trades)
        wr = sum(1 for t in trades if t.pnl_net > 0) / len(trades)
        print(f"  {side:4s}  {len(trades):4d} trades  win={wr*100:.0f}%  total=${total:+.2f}  avg=${avg:+.4f}")
    print()

    # --- Hold time distribution ---
    hold_ticks = [t.hold_ticks for t in result.trades]
    if hold_ticks:
        avg_hold = sum(hold_ticks) / len(hold_ticks)
        max_hold = max(hold_ticks)
        min_hold = min(hold_ticks)
        print(f"  --- Hold Time (ticks) ---")
        print(f"  avg={avg_hold:.1f}  min={min_hold}  max={max_hold}")
        print()

    # --- Equity curve summary ---
    if result.equity_curve:
        final = result.equity_curve[-1]
        peak = max(result.equity_curve)
        trough = min(result.equity_curve)
        print(f"  --- Equity ---")
        print(f"  Start: ${result.equity_curve[0]:.2f}  Final: ${final:.2f}  Peak: ${peak:.2f}  Trough: ${trough:.2f}")
        print()

    # --- Sample trades (last 5) ---
    print(f"  --- Last 5 Trades ---")
    for t in result.trades[-5:]:
        print(f"  [{t.exit_type:12s}] {t.side} entry=${t.entry_price:.4f} exit=${t.exit_price:.4f} "
              f"sz={t.size} pnl=${t.pnl_net:+.4f} hold={t.hold_ticks}t "
              f"bucket={t.time_bucket or '?'}")

    print(f"\n{'='*70}")
    print(f"  Report complete.")
    print(f"{'='*70}\n")


def print_sweep_report(results: list[BacktestResult]) -> None:
    """Print a summary of top parameter sweep results."""
    print(f"\n{'='*70}")
    print(f"  PARAMETER SWEEP RESULTS (top {len(results)})")
    print(f"{'='*70}\n")

    for i, r in enumerate(results, 1):
        params_str = "  ".join(f"{k}={v}" for k, v in r.params.items())
        print(f"  {i:2d}. Sharpe={r.sharpe:+.4f}  PnL=${r.total_pnl:+.2f}  "
              f"Trades={r.total_trades}  WR={r.win_rate*100:.0f}%  DD=${r.max_drawdown:.2f}")
        print(f"      {params_str}")
    print()

    # Print full report for #1
    if results:
        print(f"  --- Detailed report for best combo ---")
        print_backtest_report(results[0])


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI: python -m latpoly.strategy.backtest <file.jsonl> [--sweep]"""
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print("Usage: python -m latpoly.strategy.backtest <file.jsonl> [file2.jsonl ...] [--sweep]")
        print()
        print("Options:")
        print("  --sweep    Run parameter sweep (tests multiple threshold combos)")
        print()
        print("Environment variables (override defaults):")
        print("  LATPOLY_STRAT_ZSCORE_THRESHOLD  Z-score entry threshold (default: 2.0)")
        print("  LATPOLY_STRAT_ENTRY_MAX_S       Max seconds before expiry (default: 300)")
        print("  LATPOLY_STRAT_ENTRY_MIN_S       Min seconds before expiry (default: 30)")
        print("  LATPOLY_STRAT_MAX_SPREAD         Max spread to enter (default: 0.08)")
        print("  LATPOLY_STRAT_MIN_DEPTH          Min depth in contracts (default: 100)")
        print("  LATPOLY_STRAT_BASE_SIZE          Position size (default: 100)")
        print("  LATPOLY_STRAT_EXIT_FRAC          Exit profit fraction (default: 0.5)")
        print("  LATPOLY_STRAT_MAX_HOLD           Max hold ticks (default: 50)")
        sys.exit(0)

    do_sweep = "--sweep" in args
    file_args = [a for a in args if not a.startswith("--")]

    if not file_args:
        print("ERROR: No input files specified.")
        sys.exit(1)

    # Load ticks
    print(f"Loading ticks from {len(file_args)} file(s)...")
    ticks = load_ticks(file_args)
    print(f"Loaded {len(ticks):,} ticks")

    if not ticks:
        print("No ticks loaded. Check file paths.")
        sys.exit(1)

    if do_sweep:
        results = run_sweep(ticks)
        print_sweep_report(results)
    else:
        cfg = StrategyConfig()
        print(f"Running backtest with default config...")
        print(f"  zscore_threshold={cfg.zscore_entry_threshold}  "
              f"entry_window=[{cfg.entry_window_min_s}s, {cfg.entry_window_max_s}s]  "
              f"min_dist_strike={cfg.min_distance_to_strike}")
        result = run_backtest(ticks, cfg=cfg)
        print_backtest_report(result)


if __name__ == "__main__":
    main()
