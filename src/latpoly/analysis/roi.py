#!/usr/bin/env python3
"""ROI analysis — execution-aware backtest comparison.

Reads enriched JSONL (from enrich.py) and runs TWO backtests:
  A) IDEAL:     standard backtest (instant fill, 0 slippage)
  B) REALISTIC: applies execution penalties from enriched fields
                (latency fill price, slippage, cluster/risk filtering)

Compares the two to answer: "how much edge survives after execution reality?"

Usage:
    python -m latpoly.analysis.roi data/enriched/*_enriched.jsonl
    python -m latpoly.analysis.roi data/enriched/*_enriched.jsonl --max-risk=0.5
    python -m latpoly.analysis.roi data/enriched/*_enriched.jsonl --cluster=A_fast_edge,D_low_edge
"""

from __future__ import annotations

import json
import math
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from latpoly.strategy.backtest import BacktestResult, Trade, run_backtest, print_backtest_report
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import StrategyEngine

# ---------------------------------------------------------------------------
# Realistic backtest: applies execution penalties to enriched ticks
# ---------------------------------------------------------------------------

# Fields injected by enrich.py that we use
_EXEC_FIELDS = [
    "exec_fill_price_yes", "exec_fill_price_no",
    "exec_slippage_yes", "exec_slippage_no",
    "exec_edge_remaining", "exec_signal_survived",
    "cluster", "execution_risk_score",
]


def apply_execution_reality(
    ticks: list[dict],
    max_risk: float = 1.0,
    allowed_clusters: Optional[set[str]] = None,
) -> list[dict]:
    """Create a modified tick list that reflects execution reality.

    For ticks that would be entry candidates:
    - Shifts YES/NO mid prices to reflect latency (using exec_fill_price)
    - Widens spreads by slippage amount
    - Marks high-risk or bad-cluster ticks as low_liquidity=True (blocks entry)

    Does NOT modify original tick dicts — creates copies.
    """
    result = []
    for tick in ticks:
        t = dict(tick)  # shallow copy

        risk = t.get("execution_risk_score")
        cluster = t.get("cluster", "E_noise")

        # Block entry for high-risk ticks or excluded clusters
        blocked = False
        if risk is not None and risk > max_risk:
            blocked = True
        if allowed_clusters and cluster not in allowed_clusters:
            blocked = True

        if blocked:
            t["low_liquidity"] = True  # engine will skip this tick

        # Adjust ask prices to reflect execution fill price (latency penalty)
        exec_fill_yes = t.get("exec_fill_price_yes")
        exec_fill_no = t.get("exec_fill_price_no")

        if exec_fill_yes is not None:
            # If execution fill is worse (higher ask), use it
            cur_ask = t.get("pm_yes_best_ask")
            if cur_ask is not None and exec_fill_yes > cur_ask:
                t["pm_yes_best_ask"] = exec_fill_yes
                # Recalc spread
                bid = t.get("pm_yes_best_bid")
                if bid is not None:
                    t["spread_yes"] = round(exec_fill_yes - bid, 4)
                # Adjust VWAP too
                if t.get("yes_vwap_ask_100") is not None:
                    slip = t.get("exec_slippage_yes") or 0
                    t["yes_vwap_ask_100"] = round(t["yes_vwap_ask_100"] + abs(slip), 4)

        if exec_fill_no is not None:
            cur_ask = t.get("pm_no_best_ask")
            if cur_ask is not None and exec_fill_no > cur_ask:
                t["pm_no_best_ask"] = exec_fill_no
                bid = t.get("pm_no_best_bid")
                if bid is not None:
                    t["spread_no"] = round(exec_fill_no - bid, 4)
                if t.get("no_vwap_ask_100") is not None:
                    slip = t.get("exec_slippage_no") or 0
                    t["no_vwap_ask_100"] = round(t["no_vwap_ask_100"] + abs(slip), 4)

        # Reduce edge_score based on decay
        edge_decay = t.get("fwd_1t_edge_decay")
        if edge_decay is not None and edge_decay < 0:
            cur_edge = t.get("edge_score") or 0
            t["edge_score"] = max(0, cur_edge + edge_decay)

        result.append(t)

    return result


# ---------------------------------------------------------------------------
# Comparison report
# ---------------------------------------------------------------------------


@dataclass
class ROIComparison:
    """Side-by-side comparison of ideal vs realistic backtest."""
    ideal: BacktestResult
    realistic: BacktestResult
    max_risk: float
    allowed_clusters: Optional[set[str]]
    elapsed_s: float = 0.0

    # Per-cluster breakdown for realistic
    cluster_trades: dict = field(default_factory=lambda: defaultdict(list))
    cluster_risk_trades: dict = field(default_factory=lambda: defaultdict(list))


def run_roi_comparison(
    ticks: list[dict],
    max_risk: float = 1.0,
    allowed_clusters: Optional[set[str]] = None,
    initial_cash: float = 100.0,
) -> ROIComparison:
    """Run ideal and realistic backtests, return comparison."""
    t0 = time.monotonic()

    # Check if ticks are enriched
    sample = ticks[0] if ticks else {}
    is_enriched = "cluster" in sample and "execution_risk_score" in sample
    if not is_enriched:
        print("  WARNING: ticks not enriched (no cluster/execution_risk_score fields)")
        print("  Run enrich.py first. Falling back to ideal-only comparison.")

    cfg = StrategyConfig()

    # A) Ideal backtest
    print("  Running IDEAL backtest...", end="", flush=True)
    ideal = run_backtest(ticks, cfg=cfg, initial_cash=initial_cash)
    print(f" {ideal.total_trades} trades", flush=True)

    # B) Realistic backtest
    print("  Running REALISTIC backtest...", end="", flush=True)
    if is_enriched:
        realistic_ticks = apply_execution_reality(ticks, max_risk, allowed_clusters)
    else:
        realistic_ticks = ticks  # no enrichment, same as ideal
    realistic = run_backtest(realistic_ticks, cfg=cfg, initial_cash=initial_cash)
    print(f" {realistic.total_trades} trades", flush=True)

    elapsed = time.monotonic() - t0

    comp = ROIComparison(
        ideal=ideal,
        realistic=realistic,
        max_risk=max_risk,
        allowed_clusters=allowed_clusters,
        elapsed_s=elapsed,
    )

    # Build cluster breakdown from realistic trades
    if is_enriched:
        for trade in realistic.trades:
            entry_idx = trade.entry_tick_idx
            if 0 <= entry_idx < len(ticks):
                cluster = ticks[entry_idx].get("cluster", "unknown")
                risk = ticks[entry_idx].get("execution_risk_score", 0)
                comp.cluster_trades[cluster].append(trade)

    return comp


def print_roi_report(comp: ROIComparison) -> None:
    """Print side-by-side comparison report."""
    ideal = comp.ideal
    real = comp.realistic

    print()
    print("=" * 70)
    print("  ROI ANALYSIS — IDEAL vs REALISTIC")
    print("=" * 70)

    # Filters applied
    if comp.max_risk < 1.0:
        print(f"  Max risk filter:     {comp.max_risk}")
    if comp.allowed_clusters:
        print(f"  Allowed clusters:    {', '.join(sorted(comp.allowed_clusters))}")
    print(f"  Elapsed:             {comp.elapsed_s:.1f}s")
    print()

    # Side-by-side metrics
    print(f"  {'Metric':<25s} {'IDEAL':>12s} {'REALISTIC':>12s} {'DELTA':>12s} {'IMPACT':>8s}")
    print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*12} {'-'*8}")

    def _row(label: str, val_i: float, val_r: float, fmt: str = ".2f", pct: bool = False):
        delta = val_r - val_i
        if pct:
            impact = ""
        elif val_i != 0:
            impact = f"{delta/abs(val_i)*100:+.0f}%"
        else:
            impact = "—"
        if pct:
            print(f"  {label:<25s} {val_i*100:>11.1f}% {val_r*100:>11.1f}% {delta*100:>+11.1f}% {impact:>8s}")
        else:
            print(f"  {label:<25s} ${val_i:>11{fmt}} ${val_r:>11{fmt}} ${delta:>+11{fmt}} {impact:>8s}")

    _row("Total PnL", ideal.total_pnl, real.total_pnl)
    _row("Avg PnL/trade", ideal.avg_pnl_per_trade, real.avg_pnl_per_trade, ".4f")
    _row("Max Drawdown", ideal.max_drawdown, real.max_drawdown)

    # Non-dollar metrics
    print(f"  {'Trades':<25s} {ideal.total_trades:>12d} {real.total_trades:>12d} "
          f"{real.total_trades - ideal.total_trades:>+12d}")
    print(f"  {'Win Rate':<25s} {ideal.win_rate*100:>11.1f}% {real.win_rate*100:>11.1f}% "
          f"{(real.win_rate - ideal.win_rate)*100:>+11.1f}%")
    print(f"  {'Sharpe':<25s} {ideal.sharpe:>12.2f} {real.sharpe:>12.2f} "
          f"{real.sharpe - ideal.sharpe:>+12.2f}")

    # ROI
    if ideal.total_ticks > 0:
        start_cash = ideal.equity_curve[0] if ideal.equity_curve else 100
        roi_ideal = ideal.total_pnl / start_cash * 100
        roi_real = real.total_pnl / start_cash * 100
        print()
        print(f"  {'ROI (Ideal)':<25s} {roi_ideal:>+11.2f}%")
        print(f"  {'ROI (Realistic)':<25s} {roi_real:>+11.2f}%")
        print(f"  {'Edge Survival':<25s} {roi_real/roi_ideal*100 if roi_ideal != 0 else 0:>11.1f}%")

    # Cluster breakdown (realistic only)
    if comp.cluster_trades:
        print()
        print("  --- PnL by Cluster (Realistic) ---")
        print(f"  {'Cluster':<20s} {'Trades':>7s} {'WR':>6s} {'PnL':>10s} {'Avg PnL':>10s}")
        print(f"  {'-'*20} {'-'*7} {'-'*6} {'-'*10} {'-'*10}")

        for cluster in sorted(comp.cluster_trades.keys()):
            trades = comp.cluster_trades[cluster]
            n = len(trades)
            wins = sum(1 for t in trades if t.pnl_net > 0)
            wr = wins / n if n > 0 else 0
            total_pnl = sum(t.pnl_net for t in trades)
            avg_pnl = total_pnl / n if n > 0 else 0
            print(f"  {cluster:<20s} {n:>7d} {wr*100:>5.1f}% ${total_pnl:>+9.2f} ${avg_pnl:>+9.4f}")

    # By exit type comparison
    print()
    print("  --- By Exit Type ---")
    print(f"  {'Type':<15s} {'Ideal':>8s} {'Real':>8s}")
    print(f"  {'-'*15} {'-'*8} {'-'*8}")

    exit_types_i = defaultdict(int)
    exit_types_r = defaultdict(int)
    for t in ideal.trades:
        exit_types_i[t.exit_type] += 1
    for t in real.trades:
        exit_types_r[t.exit_type] += 1

    all_types = sorted(set(exit_types_i) | set(exit_types_r))
    for et in all_types:
        print(f"  {et:<15s} {exit_types_i.get(et, 0):>8d} {exit_types_r.get(et, 0):>8d}")

    # Verdict
    print()
    print("  --- VERDICT ---")
    if real.total_trades == 0:
        print("  No trades in realistic mode. Filters too aggressive.")
    elif real.total_pnl > 0 and ideal.total_pnl > 0:
        survival = real.total_pnl / ideal.total_pnl * 100
        if survival >= 70:
            verdict = "STRONG — edge survives execution well"
        elif survival >= 40:
            verdict = "MODERATE — significant execution cost but still profitable"
        elif survival > 0:
            verdict = "WEAK — most edge consumed by execution"
        else:
            verdict = "NEGATIVE — execution destroys all edge"
        print(f"  PnL survival: {survival:.1f}% — {verdict}")
    elif real.total_pnl <= 0 and ideal.total_pnl > 0:
        print(f"  FAIL — ideal profitable (${ideal.total_pnl:+.2f}) but execution makes it negative (${real.total_pnl:+.2f})")
    elif ideal.total_pnl <= 0:
        print(f"  FAIL — even ideal backtest is not profitable (${ideal.total_pnl:+.2f})")
    else:
        print(f"  Ideal: ${ideal.total_pnl:+.2f}  Realistic: ${real.total_pnl:+.2f}")

    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI: python -m latpoly.analysis.roi <enriched.jsonl> [options]"""
    args = sys.argv[1:]

    if not args or "--help" in args or "-h" in args:
        print(__doc__)
        sys.exit(0)

    # Parse arguments
    input_files: list[str] = []
    max_risk: float = 1.0
    allowed_clusters: Optional[set[str]] = None
    initial_cash: float = 100.0

    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--max-risk="):
            max_risk = float(a.split("=", 1)[1])
        elif a.startswith("--cluster="):
            allowed_clusters = set(a.split("=", 1)[1].split(","))
        elif a.startswith("--cash="):
            initial_cash = float(a.split("=", 1)[1])
        elif not a.startswith("--"):
            input_files.append(a)
        i += 1

    if not input_files:
        print("ERROR: no input files specified")
        sys.exit(1)

    # Load enriched ticks
    from latpoly.analysis.lag_report import load_ticks
    print(f"Loading enriched ticks from {len(input_files)} file(s)...")
    ticks = load_ticks(input_files)
    print(f"Loaded {len(ticks):,} ticks")

    if not ticks:
        print("No ticks loaded.")
        sys.exit(1)

    # Run comparison
    comp = run_roi_comparison(
        ticks,
        max_risk=max_risk,
        allowed_clusters=allowed_clusters,
        initial_cash=initial_cash,
    )

    print_roi_report(comp)

    # Also show cluster-only analysis if no filter applied
    if max_risk >= 1.0 and allowed_clusters is None:
        print()
        print("  TIP: Try filtering by cluster A only:")
        print("    python -m latpoly.analysis.roi <files> --cluster=A_fast_edge")
        print("  Or limit execution risk:")
        print("    python -m latpoly.analysis.roi <files> --max-risk=0.3")


if __name__ == "__main__":
    main()
