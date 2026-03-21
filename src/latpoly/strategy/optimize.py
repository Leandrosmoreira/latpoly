#!/usr/bin/env python3
"""Optuna-based parameter optimization for the latpoly strategy.

Uses TPE (Tree-structured Parzen Estimator) for intelligent search over
the continuous parameter space — much more efficient than grid search.

Usage:
    python -m latpoly.strategy.optimize data/daily/*.jsonl
    python -m latpoly.strategy.optimize data/daily/*.jsonl --n-trials=200 --seed=42
    python -m latpoly.strategy.optimize data/daily/*.jsonl --storage=data/optuna/study.journal
"""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING

from latpoly.strategy.backtest import (
    BacktestResult,
    _make_config_with_overrides,
    print_backtest_report,
    run_backtest,
)

if TYPE_CHECKING:
    import optuna


# ---------------------------------------------------------------------------
# Search space
# ---------------------------------------------------------------------------


def suggest_params(trial: optuna.Trial) -> dict:
    """Sample strategy parameters from the search space.

    Parameter names match StrategyConfig fields exactly so they can be
    passed directly to ``_make_config_with_overrides()``.

    24 tunable parameters — excludes fixed Polymarket rules
    (fee structure, min_maker_size, entry_as_maker) and initial_bankroll.
    """
    return {
        # -- Entry thresholds --
        "zscore_entry_threshold": trial.suggest_float("zscore_entry_threshold", 1.0, 3.5),
        "btc_to_pm_base_rate": trial.suggest_float("btc_to_pm_base_rate", 0.0005, 0.005, log=True),
        "min_bn_move_abs": trial.suggest_float("min_bn_move_abs", 2.0, 12.0),
        "min_ret_1s_confirm": trial.suggest_float("min_ret_1s_confirm", 0.0, 1.0),
        "min_mid_entry": trial.suggest_float("min_mid_entry", 0.05, 0.30),
        "max_mid_entry": trial.suggest_float("max_mid_entry", 0.30, 0.90),
        "max_spread_entry": trial.suggest_float("max_spread_entry", 0.03, 0.15),
        "min_depth_contracts": trial.suggest_float("min_depth_contracts", 30.0, 300.0),
        "min_net_edge": trial.suggest_float("min_net_edge", 0.001, 0.02, log=True),
        # -- Risk filters --
        "min_distance_to_strike": trial.suggest_float("min_distance_to_strike", 1.0, 20.0),
        "max_data_age_ms": trial.suggest_float("max_data_age_ms", 500.0, 5000.0),
        # -- Timing --
        "cooldown_ticks": trial.suggest_int("cooldown_ticks", 2, 30),
        "entry_window_min_s": trial.suggest_float("entry_window_min_s", 10.0, 90.0),
        "entry_window_max_s": trial.suggest_float("entry_window_max_s", 200.0, 900.0),
        # -- Position sizing --
        "base_size_contracts": 5,  # fixed: min Polymarket maker size
        "max_concurrent_positions": trial.suggest_int("max_concurrent_positions", 1, 15),
        "max_exposure_frac": trial.suggest_float("max_exposure_frac", 0.2, 0.8),
        # -- Exit strategy --
        "exit_profit_fraction": trial.suggest_float("exit_profit_fraction", 0.2, 0.9),
        "max_hold_ticks": trial.suggest_int("max_hold_ticks", 15, 120),
        "stop_loss_per_contract": trial.suggest_float("stop_loss_per_contract", 0.01, 0.08),
        "hold_to_expiry_distance": trial.suggest_float("hold_to_expiry_distance", 15.0, 150.0),
        # -- Time weight (aggression near expiry) --
        "time_weight_min": trial.suggest_float("time_weight_min", 0.5, 2.0),
        "time_weight_max": trial.suggest_float("time_weight_max", 1.0, 5.0),
        # -- Daily limits --
        "max_daily_loss": trial.suggest_float("max_daily_loss", 10.0, 100.0),
        "max_daily_trades": trial.suggest_int("max_daily_trades", 50, 5000),
    }


# ---------------------------------------------------------------------------
# Objective function
# ---------------------------------------------------------------------------


def create_objective(
    ticks: list[dict],
    initial_cash: float = 1000.0,
):
    """Return an Optuna objective function with captured tick data.

    Score = total_pnl * trade_confidence - dd_penalty
    - trade_confidence ramps from 0→1 as total_trades goes 0→50
    - dd_penalty = 2.0 * |max_drawdown|
    - Configs with <5 trades get score -10 (garbage)
    - Uses PnL directly (not Sharpe) to avoid rewarding zero-variance configs
    """

    def objective(trial: optuna.Trial) -> float:
        params = suggest_params(trial)
        cfg = _make_config_with_overrides(params)
        result = run_backtest(ticks, cfg=cfg, initial_cash=initial_cash, params=params)

        # Store metrics for later inspection
        trial.set_user_attr("total_trades", result.total_trades)
        trial.set_user_attr("total_pnl", round(result.total_pnl, 4))
        trial.set_user_attr("win_rate", round(result.win_rate, 4))
        trial.set_user_attr("max_drawdown", round(result.max_drawdown, 4))
        trial.set_user_attr("sharpe", round(result.sharpe, 4))
        trial.set_user_attr("avg_pnl_per_trade", round(result.avg_pnl_per_trade, 6))

        if result.total_trades < 5:
            return -10.0

        trade_confidence = min(1.0, result.total_trades / 50.0)
        dd_penalty = 2.0 * abs(result.max_drawdown)
        score = result.total_pnl * trade_confidence - dd_penalty
        return score

    return objective


# ---------------------------------------------------------------------------
# Study runner
# ---------------------------------------------------------------------------


def run_optimize(
    ticks: list[dict],
    n_trials: int = 500,
    study_name: str = "latpoly",
    storage_path: str | None = None,
    seed: int = 42,
    initial_cash: float = 100.0,
    n_jobs: int = 2,
    verbose: bool = True,
) -> optuna.Study:
    """Run Optuna optimization over the strategy parameter space.

    Args:
        ticks: Pre-loaded tick data (list of dicts from JSONL).
        n_trials: Number of trials to run.
        study_name: Name for the Optuna study (used for resume).
        storage_path: Path to JournalFile for persistence. None = in-memory.
        seed: Random seed for reproducibility.
        initial_cash: Starting virtual cash for backtest.
        verbose: Print progress info.

    Returns:
        The completed Optuna study.
    """
    import optuna

    # Configure logging
    if not verbose:
        optuna.logging.set_verbosity(optuna.logging.WARNING)

    # Storage
    storage = None
    if storage_path:
        storage = optuna.storages.JournalStorage(
            optuna.storages.JournalFileStorage(storage_path)
        )

    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(
        study_name=study_name,
        storage=storage,
        direction="maximize",
        sampler=sampler,
        load_if_exists=True,
    )

    # Record metadata
    study.set_user_attr("seed", seed)
    study.set_user_attr("n_ticks", len(ticks))
    study.set_user_attr("initial_cash", initial_cash)

    existing = len(study.trials)
    if existing and verbose:
        print(f"  Resuming study '{study_name}' with {existing} existing trials")

    objective = create_objective(ticks, initial_cash=initial_cash)

    t0 = time.monotonic()
    study.optimize(objective, n_trials=n_trials, n_jobs=n_jobs)
    elapsed = time.monotonic() - t0

    if verbose:
        total = len(study.trials)
        print(f"\n  Completed {n_trials} trials in {elapsed:.1f}s "
              f"({elapsed/n_trials*1000:.0f}ms/trial, {total} total)")

    return study


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_optimize_report(study: optuna.Study, top_n: int = 10) -> None:
    """Print a formatted report of the optimization results."""
    trials = sorted(study.trials, key=lambda t: t.value if t.value is not None else -999, reverse=True)
    valid = [t for t in trials if t.value is not None and t.value > -10]

    print(f"\n{'='*70}")
    print(f"  OPTUNA OPTIMIZATION REPORT")
    print(f"  Study: {study.study_name}  |  Total trials: {len(study.trials)}  |  Valid: {len(valid)}")
    print(f"{'='*70}\n")

    if not valid:
        print("  No valid trials found. All configs produced <3 trades.\n")
        return

    # Top N trials
    print(f"  --- Top {min(top_n, len(valid))} Trials ---\n")
    print(f"  {'#':>3s}  {'Score':>7s}  {'Sharpe':>7s}  {'PnL':>8s}  {'Trades':>6s}  "
          f"{'WR':>5s}  {'DD':>7s}  {'Avg PnL':>9s}")
    print(f"  {'-'*3}  {'-'*7}  {'-'*7}  {'-'*8}  {'-'*6}  {'-'*5}  {'-'*7}  {'-'*9}")

    for i, t in enumerate(valid[:top_n], 1):
        ua = t.user_attrs
        print(f"  {i:3d}  {t.value:+7.3f}  {ua.get('sharpe', 0):+7.3f}  "
              f"${ua.get('total_pnl', 0):+7.2f}  {ua.get('total_trades', 0):6d}  "
              f"{ua.get('win_rate', 0)*100:4.0f}%  "
              f"${ua.get('max_drawdown', 0):6.2f}  "
              f"${ua.get('avg_pnl_per_trade', 0):+8.5f}")
    print()

    # Best parameters
    best = study.best_trial
    print(f"  --- Best Parameters (Trial #{best.number}, Score={best.value:+.4f}) ---\n")
    for name, value in sorted(best.params.items()):
        if isinstance(value, float):
            if abs(value) < 0.01:
                print(f"    {name:30s} = {value:.6f}")
            else:
                print(f"    {name:30s} = {value:.4f}")
        else:
            print(f"    {name:30s} = {value}")
    print()

    # Compare with defaults
    from latpoly.strategy.config import StrategyConfig
    defaults = StrategyConfig()
    print(f"  --- Best vs Default ---\n")
    print(f"  {'Parameter':30s}  {'Default':>10s}  {'Best':>10s}  {'Delta':>10s}")
    print(f"  {'-'*30}  {'-'*10}  {'-'*10}  {'-'*10}")
    for name in sorted(best.params.keys()):
        default_val = getattr(defaults, name, None)
        best_val = best.params[name]
        if default_val is not None:
            if isinstance(best_val, float):
                delta = best_val - default_val
                print(f"  {name:30s}  {default_val:10.4f}  {best_val:10.4f}  {delta:+10.4f}")
            else:
                delta = best_val - default_val
                print(f"  {name:30s}  {default_val:10d}  {best_val:10d}  {delta:+10d}")
    print()

    # Run full backtest report on best config
    print(f"  --- Full Backtest Report (Best Config) ---")
    cfg = _make_config_with_overrides(best.params)
    ticks_count = study.user_attrs.get("n_ticks", 0)
    initial_cash = study.user_attrs.get("initial_cash", 1000.0)
    print(f"  (Stored metrics from trial — {ticks_count:,} ticks, ${initial_cash:.0f} cash)")

    ua = best.user_attrs
    print(f"\n  Trades: {ua.get('total_trades', 0)}  |  "
          f"WR: {ua.get('win_rate', 0)*100:.1f}%  |  "
          f"PnL: ${ua.get('total_pnl', 0):+.2f}  |  "
          f"Sharpe: {ua.get('sharpe', 0):+.4f}  |  "
          f"DD: ${ua.get('max_drawdown', 0):.2f}")
    print()

    # Parameter importance (if enough trials)
    if len(valid) >= 20:
        try:
            import optuna
            importance = optuna.importance.get_param_importances(study)
            print(f"  --- Parameter Importance ---\n")
            for name, imp in sorted(importance.items(), key=lambda x: -x[1])[:10]:
                bar = "#" * int(imp * 40)
                print(f"    {name:30s}  {imp:.3f}  {bar}")
            print()
        except Exception:
            pass  # importance computation can fail with few trials

    print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI: python -m latpoly.strategy.optimize <file.jsonl> [options]"""
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print("Usage: python -m latpoly.strategy.optimize <file.jsonl> [file2.jsonl ...] [options]")
        print()
        print("Options:")
        print("  --n-trials=N      Number of Optuna trials (default: 200)")
        print("  --seed=N          Random seed for reproducibility (default: 42)")
        print("  --study=NAME      Study name for resume (default: latpoly)")
        print("  --storage=PATH    JournalFile path for persistence (default: in-memory)")
        print("  --slot=SLOT_ID    Filter ticks by slot_id (e.g. --slot=btc-15m)")
        print("  --jobs=N          Parallel threads (default: 4)")
        print("  --cash=N          Initial cash (default: 100)")
        print("  --quiet           Suppress Optuna trial-by-trial logging")
        sys.exit(0)

    # Parse options
    n_trials = 500
    seed = 42
    study_name = "latpoly"
    storage_path = None
    slot_filter = None
    initial_cash = 100.0
    n_jobs = 2
    quiet = "--quiet" in args

    for a in args:
        if a.startswith("--n-trials="):
            n_trials = int(a.split("=", 1)[1])
        elif a.startswith("--seed="):
            seed = int(a.split("=", 1)[1])
        elif a.startswith("--study="):
            study_name = a.split("=", 1)[1]
        elif a.startswith("--storage="):
            storage_path = a.split("=", 1)[1]
        elif a.startswith("--slot="):
            slot_filter = a.split("=", 1)[1]
        elif a.startswith("--cash="):
            initial_cash = float(a.split("=", 1)[1])
        elif a.startswith("--jobs="):
            n_jobs = int(a.split("=", 1)[1])

    file_args = [a for a in args if not a.startswith("--")]

    if not file_args:
        print("ERROR: No input files specified.")
        sys.exit(1)

    # Load ticks
    print(f"Loading ticks from {len(file_args)} file(s)...")
    if slot_filter:
        from latpoly.strategy.backtest import _load_ticks_filtered
        ticks = _load_ticks_filtered(file_args, slot_filter)
        print(f"Loaded {len(ticks):,} ticks (filtered to slot '{slot_filter}')")
    else:
        from latpoly.analysis.lag_report import load_ticks
        ticks = load_ticks(file_args)
        print(f"Loaded {len(ticks):,} ticks")

    if not ticks:
        print("No ticks loaded. Check file paths.")
        sys.exit(1)

    # Ensure storage directory exists
    if storage_path:
        from pathlib import Path
        Path(storage_path).parent.mkdir(parents=True, exist_ok=True)

    # Run optimization
    print(f"\n  Optuna TPE optimization: {n_trials} trials, seed={seed}, jobs={n_jobs}")
    print(f"  Study: {study_name}  |  Storage: {storage_path or 'in-memory'}")
    print(f"  Search space: 24 parameters (all tunable StrategyConfig fields)")
    print()

    study = run_optimize(
        ticks,
        n_trials=n_trials,
        study_name=study_name,
        storage_path=storage_path,
        seed=seed,
        initial_cash=initial_cash,
        n_jobs=n_jobs,
        verbose=not quiet,
    )

    print_optimize_report(study)


if __name__ == "__main__":
    main()
