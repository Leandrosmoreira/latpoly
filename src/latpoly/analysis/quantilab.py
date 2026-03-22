#!/usr/bin/env python3
"""QuantiLab -- unified pipeline: Optimize > Enrich > ROI > Verdict.

Orchestrates the full analysis pipeline:
  Stage 1: Optuna parameter optimization (best params)
  Stage 2: Enriched log generation (execution simulation + clusters)
  Stage 3: ROI comparison (ideal vs realistic)
  Stage 4: Final verdict (APPROVED / NEEDS_ADJUSTMENT / NOT_VIABLE)

Usage:
    python -m latpoly.analysis.quantilab data/daily/2026-03-18_merged.jsonl --slot=btc-15m --n-trials=200
    python -m latpoly.analysis.quantilab data/daily/*.jsonl --slot=btc-15m --stage=optimize
    python -m latpoly.analysis.quantilab data/enriched/*_enriched.jsonl --stage=roi
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARTIFACTS_DIR = Path("artifacts/quantilab")

# Verdict thresholds
MIN_DECAY_RATIO_APPROVED = 0.70
MIN_DECAY_RATIO_ADJUST = 0.40
MIN_WIN_RATE = 0.60
MAX_DD_FRACTION = 0.20  # max drawdown as fraction of bankroll


# ---------------------------------------------------------------------------
# Run config
# ---------------------------------------------------------------------------


@dataclass
class RunConfig:
    input_files: list[str] = field(default_factory=list)
    slot_filter: Optional[str] = None
    stage: str = "full"  # full, optimize, enrich, roi
    n_trials: int = 200
    n_jobs: int = 1
    seed: int = 42
    latency_profile: str = "medium"
    max_risk: float = 1.0
    initial_cash: float = 100.0
    run_id: str = ""

    def __post_init__(self):
        if not self.run_id:
            self.run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")


# ---------------------------------------------------------------------------
# Stage 1: Optimize
# ---------------------------------------------------------------------------


def stage_optimize(ticks: list[dict], cfg: RunConfig, run_dir: Path) -> dict:
    """Run Optuna optimization, return best params + summary."""
    from latpoly.strategy.optimize import run_optimize

    print(f"\n{'='*60}")
    print(f"  STAGE 1: OPTIMIZATION")
    print(f"{'='*60}")

    study = run_optimize(
        ticks,
        n_trials=cfg.n_trials,
        seed=cfg.seed,
        initial_cash=cfg.initial_cash,
        n_jobs=cfg.n_jobs,
        verbose=True,
    )

    best = study.best_trial
    summary = {
        "stage": "optimize",
        "n_trials": len(study.trials),
        "n_valid": len([t for t in study.trials if t.value is not None]),
        "best_trial": best.number,
        "best_score": round(best.value, 4),
        "best_params": best.params,
        "best_metrics": {
            k: best.user_attrs[k]
            for k in ["total_pnl", "sharpe", "max_drawdown", "win_rate",
                       "total_trades", "avg_pnl_per_trade"]
            if k in best.user_attrs
        },
        "top_5": [
            {
                "trial": t.number,
                "score": round(t.value, 4),
                "pnl": t.user_attrs.get("total_pnl", 0),
                "trades": t.user_attrs.get("total_trades", 0),
                "win_rate": t.user_attrs.get("win_rate", 0),
            }
            for t in sorted(study.trials, key=lambda t: t.value or -999, reverse=True)[:5]
        ],
    }

    # Save artifact
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "01_optuna_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Best trial #{best.number}: score={best.value:.4f}")
    print(f"  Saved: {run_dir / '01_optuna_summary.json'}")

    return summary


# ---------------------------------------------------------------------------
# Stage 2: Enrich
# ---------------------------------------------------------------------------


def stage_enrich(cfg: RunConfig, run_dir: Path) -> dict:
    """Run enrichment on input files, return summary."""
    from latpoly.analysis.enrich import enrich_file, EnrichSummary

    print(f"\n{'='*60}")
    print(f"  STAGE 2: ENRICHMENT")
    print(f"{'='*60}")

    enriched_dir = Path("data/enriched")
    enriched_dir.mkdir(parents=True, exist_ok=True)

    summary_obj = EnrichSummary()
    enriched_files: list[str] = []
    t0 = time.monotonic()

    for fpath in cfg.input_files:
        inp = Path(fpath)
        if not inp.exists():
            print(f"  WARNING: {fpath} not found, skipping")
            continue

        out_name = inp.stem.replace("_merged", "") + "_enriched.jsonl"
        out_path = enriched_dir / out_name

        enrich_file(inp, out_path, cfg.latency_profile, cfg.slot_filter, summary_obj)
        enriched_files.append(str(out_path))

    elapsed = time.monotonic() - t0
    summary_obj.elapsed_s = elapsed

    summary = {
        "stage": "enrich",
        "total_ticks": summary_obj.total_ticks,
        "enriched_ticks": summary_obj.enriched_ticks,
        "skipped_slot": summary_obj.skipped_slot,
        "elapsed_s": round(elapsed, 1),
        "latency_profile": cfg.latency_profile,
        "cluster_distribution": {
            k: {
                "count": summary_obj.cluster_counts[k],
                "pct": round(summary_obj.cluster_counts[k] / max(summary_obj.enriched_ticks, 1) * 100, 1),
                "avg_risk": round(summary_obj.cluster_risk_sums[k] / max(summary_obj.cluster_counts[k], 1), 4),
            }
            for k in sorted(summary_obj.cluster_counts.keys())
        },
        "signal_survived": summary_obj.signal_survived_count,
        "signal_total": summary_obj.signal_total_count,
        "signal_survival_rate": round(
            summary_obj.signal_survived_count / max(summary_obj.signal_total_count, 1) * 100, 1
        ),
        "enriched_files": enriched_files,
    }

    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "02_enrich_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Enriched {summary_obj.enriched_ticks:,} ticks in {elapsed:.1f}s")
    print(f"  Signal survival: {summary['signal_survival_rate']}%")
    print(f"  Saved: {run_dir / '02_enrich_summary.json'}")

    return summary


# ---------------------------------------------------------------------------
# Stage 3: ROI
# ---------------------------------------------------------------------------


def stage_roi(cfg: RunConfig, run_dir: Path, enriched_files: list[str] | None = None) -> dict:
    """Run ROI comparison on enriched files, return summary."""
    from latpoly.analysis.lag_report import load_ticks
    from latpoly.analysis.roi import run_roi_comparison, print_roi_report

    print(f"\n{'='*60}")
    print(f"  STAGE 3: ROI ANALYSIS")
    print(f"{'='*60}")

    # Determine which files to load
    if enriched_files:
        files_to_load = enriched_files
    else:
        # If stage=roi, input_files should be enriched files directly
        files_to_load = cfg.input_files

    print(f"  Loading enriched ticks from {len(files_to_load)} file(s)...")
    ticks = load_ticks(files_to_load)
    print(f"  Loaded {len(ticks):,} ticks")

    if not ticks:
        print("  WARNING: No ticks loaded for ROI analysis")
        return {"stage": "roi", "error": "no_ticks"}

    comp = run_roi_comparison(
        ticks,
        max_risk=cfg.max_risk,
        initial_cash=cfg.initial_cash,
    )

    print_roi_report(comp)

    # Build summary
    ideal = comp.ideal
    real = comp.realistic
    decay_ratio = real.total_pnl / ideal.total_pnl if ideal.total_pnl > 0 else 0

    summary = {
        "stage": "roi",
        "ideal": {
            "trades": ideal.total_trades,
            "pnl": round(ideal.total_pnl, 4),
            "win_rate": round(ideal.win_rate, 4),
            "sharpe": round(ideal.sharpe, 4),
            "max_drawdown": round(ideal.max_drawdown, 4),
        },
        "realistic": {
            "trades": real.total_trades,
            "pnl": round(real.total_pnl, 4),
            "win_rate": round(real.win_rate, 4),
            "sharpe": round(real.sharpe, 4),
            "max_drawdown": round(real.max_drawdown, 4),
        },
        "execution_decay_ratio": round(decay_ratio, 4),
        "edge_survival_pct": round(decay_ratio * 100, 1),
        "cluster_pnl": {
            cluster: {
                "trades": len(trades),
                "pnl": round(sum(t.pnl_net for t in trades), 4),
                "win_rate": round(sum(1 for t in trades if t.pnl_net > 0) / max(len(trades), 1), 4),
            }
            for cluster, trades in comp.cluster_trades.items()
        },
    }

    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "03_roi_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Saved: {run_dir / '03_roi_summary.json'}")

    # Free memory
    del ticks
    return summary


# ---------------------------------------------------------------------------
# Stage 4: Verdict
# ---------------------------------------------------------------------------


def generate_verdict(
    optimize_summary: dict | None,
    enrich_summary: dict | None,
    roi_summary: dict | None,
    initial_cash: float = 100.0,
) -> dict:
    """Generate final verdict based on all stage results."""
    reasons: list[str] = []
    verdict = "NOT_VIABLE"

    if roi_summary is None or "error" in roi_summary:
        return {
            "verdict": "NOT_VIABLE",
            "reasons": ["ROI analysis failed or no data"],
        }

    ideal = roi_summary.get("ideal", {})
    real = roi_summary.get("realistic", {})
    decay_ratio = roi_summary.get("execution_decay_ratio", 0)

    ideal_pnl = ideal.get("pnl", 0)
    real_pnl = real.get("pnl", 0)
    real_wr = real.get("win_rate", 0)
    real_dd = real.get("max_drawdown", 0)
    real_trades = real.get("trades", 0)

    # Check conditions
    if real_pnl <= 0:
        verdict = "NOT_VIABLE"
        reasons.append(f"Realistic PnL negative: ${real_pnl:.2f}")
    elif ideal_pnl <= 0:
        verdict = "NOT_VIABLE"
        reasons.append(f"Even ideal PnL is negative: ${ideal_pnl:.2f}")
    elif (decay_ratio >= MIN_DECAY_RATIO_APPROVED
          and real_wr >= MIN_WIN_RATE
          and real_dd < initial_cash * MAX_DD_FRACTION):
        verdict = "APPROVED_FOR_LIVE"
        reasons.append(f"Edge survival {decay_ratio*100:.1f}% (>= {MIN_DECAY_RATIO_APPROVED*100:.0f}%)")
        reasons.append(f"Win rate {real_wr*100:.1f}% (>= {MIN_WIN_RATE*100:.0f}%)")
        reasons.append(f"Max drawdown ${real_dd:.2f} (< ${initial_cash * MAX_DD_FRACTION:.2f})")
    elif decay_ratio >= MIN_DECAY_RATIO_ADJUST and real_pnl > 0:
        verdict = "NEEDS_ADJUSTMENT"
        if decay_ratio < MIN_DECAY_RATIO_APPROVED:
            reasons.append(f"Edge survival {decay_ratio*100:.1f}% (< {MIN_DECAY_RATIO_APPROVED*100:.0f}%)")
        if real_wr < MIN_WIN_RATE:
            reasons.append(f"Win rate {real_wr*100:.1f}% (< {MIN_WIN_RATE*100:.0f}%)")
        if real_dd >= initial_cash * MAX_DD_FRACTION:
            reasons.append(f"Max drawdown ${real_dd:.2f} (>= ${initial_cash * MAX_DD_FRACTION:.2f})")
    else:
        verdict = "NOT_VIABLE"
        reasons.append(f"Edge survival {decay_ratio*100:.1f}% too low")

    # Add context
    if real_trades < 10:
        reasons.append(f"Warning: only {real_trades} trades — low confidence")

    if enrich_summary:
        surv = enrich_summary.get("signal_survival_rate", 0)
        reasons.append(f"Signal survival rate: {surv}%")

    # Best cluster
    cluster_pnl = roi_summary.get("cluster_pnl", {})
    if cluster_pnl:
        best_cluster = max(cluster_pnl.items(), key=lambda x: x[1].get("pnl", 0))
        reasons.append(f"Best cluster: {best_cluster[0]} (${best_cluster[1]['pnl']:+.2f})")

    return {
        "verdict": verdict,
        "execution_decay_ratio": round(decay_ratio, 4),
        "ideal_pnl": round(ideal_pnl, 4),
        "realistic_pnl": round(real_pnl, 4),
        "realistic_trades": real_trades,
        "realistic_win_rate": round(real_wr, 4),
        "realistic_drawdown": round(real_dd, 4),
        "reasons": reasons,
    }


# ---------------------------------------------------------------------------
# Final Report
# ---------------------------------------------------------------------------


def print_final_report(
    cfg: RunConfig,
    optimize_summary: dict | None,
    enrich_summary: dict | None,
    roi_summary: dict | None,
    verdict: dict,
    run_dir: Path,
) -> str:
    """Print and return the final consolidated report."""
    lines: list[str] = []

    def p(s: str = ""):
        lines.append(s)
        print(s)

    p()
    p("=" * 60)
    p("  QUANTILAB — FINAL REPORT")
    p(f"  Run ID: {cfg.run_id}")
    if cfg.slot_filter:
        p(f"  Slot: {cfg.slot_filter}")
    p(f"  Files: {len(cfg.input_files)} | Cash: ${cfg.initial_cash:.0f}")
    p("=" * 60)

    # Stage 1
    if optimize_summary:
        p()
        p("  ── Stage 1: Optimization ──")
        p(f"  Trials: {optimize_summary['n_trials']} | "
          f"Valid: {optimize_summary['n_valid']} | "
          f"Best: #{optimize_summary['best_trial']} (score={optimize_summary['best_score']:+.4f})")
        metrics = optimize_summary.get("best_metrics", {})
        if metrics:
            p(f"  PnL: ${metrics.get('total_pnl', 0):.2f} | "
              f"Trades: {metrics.get('total_trades', 0)} | "
              f"WR: {metrics.get('win_rate', 0)*100:.1f}%")

    # Stage 2
    if enrich_summary:
        p()
        p("  ── Stage 2: Enrichment ──")
        p(f"  Ticks: {enrich_summary['enriched_ticks']:,} | "
          f"Rate: {enrich_summary['enriched_ticks'] / max(enrich_summary['elapsed_s'], 0.1):,.0f}/sec | "
          f"Profile: {enrich_summary['latency_profile']}")
        clusters = enrich_summary.get("cluster_distribution", {})
        if clusters:
            parts = [f"{k}={v['pct']}%" for k, v in clusters.items() if v['pct'] >= 1.0]
            p(f"  Clusters: {', '.join(parts)}")
        p(f"  Signal survival: {enrich_summary.get('signal_survival_rate', 0)}%")

    # Stage 3
    if roi_summary and "error" not in roi_summary:
        p()
        p("  ── Stage 3: ROI Analysis ──")
        ideal = roi_summary["ideal"]
        real = roi_summary["realistic"]
        p(f"  {'Metric':<20s} {'IDEAL':>10s} {'REALISTIC':>10s}")
        p(f"  {'-'*20} {'-'*10} {'-'*10}")
        p(f"  {'Trades':<20s} {ideal['trades']:>10d} {real['trades']:>10d}")
        p(f"  {'PnL':<20s} ${ideal['pnl']:>9.2f} ${real['pnl']:>9.2f}")
        p(f"  {'Win Rate':<20s} {ideal['win_rate']*100:>9.1f}% {real['win_rate']*100:>9.1f}%")
        p(f"  {'Sharpe':<20s} {ideal['sharpe']:>10.2f} {real['sharpe']:>10.2f}")
        p(f"  {'Max DD':<20s} ${ideal['max_drawdown']:>9.2f} ${real['max_drawdown']:>9.2f}")
        p(f"  Edge Survival: {roi_summary['edge_survival_pct']}%")

    # Stage 4: Verdict
    p()
    p("  ── Stage 4: Verdict ──")
    v = verdict["verdict"]

    if v == "APPROVED_FOR_LIVE":
        box_char = "+"
    elif v == "NEEDS_ADJUSTMENT":
        box_char = "~"
    else:
        box_char = "!"

    p()
    p(f"  {box_char * 50}")
    p(f"  {box_char}  VERDICT: {v:<39s}{box_char}")
    p(f"  {box_char}{' ' * 48}{box_char}")
    for reason in verdict.get("reasons", []):
        line = f"  {box_char}  {reason:<46s}{box_char}"
        p(line)
    p(f"  {box_char * 50}")
    p()
    p("=" * 60)

    report_text = "\n".join(lines)

    # Save report
    run_dir.mkdir(parents=True, exist_ok=True)
    with open(run_dir / "final_report.txt", "w") as f:
        f.write(report_text)

    return report_text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI: python -m latpoly.analysis.quantilab <files> [options]"""
    args = sys.argv[1:]

    if not args or "--help" in args or "-h" in args:
        print(__doc__)
        sys.exit(0)

    cfg = RunConfig()

    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--slot="):
            cfg.slot_filter = a.split("=", 1)[1]
        elif a.startswith("--stage="):
            cfg.stage = a.split("=", 1)[1]
        elif a.startswith("--n-trials="):
            cfg.n_trials = int(a.split("=", 1)[1])
        elif a.startswith("--jobs="):
            cfg.n_jobs = int(a.split("=", 1)[1])
        elif a.startswith("--seed="):
            cfg.seed = int(a.split("=", 1)[1])
        elif a.startswith("--latency-profile="):
            cfg.latency_profile = a.split("=", 1)[1]
        elif a.startswith("--max-risk="):
            cfg.max_risk = float(a.split("=", 1)[1])
        elif a.startswith("--cash="):
            cfg.initial_cash = float(a.split("=", 1)[1])
        elif a.startswith("--run-id="):
            cfg.run_id = a.split("=", 1)[1]
        elif not a.startswith("--"):
            cfg.input_files.append(a)
        i += 1

    if not cfg.input_files:
        print("ERROR: no input files specified")
        sys.exit(1)

    run_dir = ARTIFACTS_DIR / cfg.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Save run config
    with open(run_dir / "00_config.json", "w") as f:
        json.dump({
            "input_files": cfg.input_files,
            "slot_filter": cfg.slot_filter,
            "stage": cfg.stage,
            "n_trials": cfg.n_trials,
            "n_jobs": cfg.n_jobs,
            "seed": cfg.seed,
            "latency_profile": cfg.latency_profile,
            "max_risk": cfg.max_risk,
            "initial_cash": cfg.initial_cash,
            "run_id": cfg.run_id,
        }, f, indent=2)

    optimize_summary = None
    enrich_summary = None
    roi_summary = None
    enriched_files = None

    stages = cfg.stage

    # ── Stage 1: Optimize ──
    if stages in ("full", "optimize"):
        from latpoly.strategy.backtest import _load_ticks_filtered
        from latpoly.analysis.lag_report import load_ticks

        print(f"Loading ticks from {len(cfg.input_files)} file(s)...")
        if cfg.slot_filter:
            ticks = _load_ticks_filtered(cfg.input_files, cfg.slot_filter)
            print(f"Loaded {len(ticks):,} ticks (filtered to slot '{cfg.slot_filter}')")
        else:
            ticks = load_ticks(cfg.input_files)
            print(f"Loaded {len(ticks):,} ticks")

        if not ticks:
            print("ERROR: No ticks loaded")
            sys.exit(1)

        optimize_summary = stage_optimize(ticks, cfg, run_dir)
        del ticks  # free memory before next stage

    # ── Stage 2: Enrich ──
    if stages in ("full", "enrich"):
        enrich_summary = stage_enrich(cfg, run_dir)
        enriched_files = enrich_summary.get("enriched_files", [])

    # ── Stage 3: ROI ──
    if stages in ("full", "roi"):
        roi_summary = stage_roi(cfg, run_dir, enriched_files)

    # ── Stage 4: Verdict ──
    if stages == "full" or (roi_summary and "error" not in (roi_summary or {})):
        verdict = generate_verdict(
            optimize_summary, enrich_summary, roi_summary,
            initial_cash=cfg.initial_cash,
        )

        run_dir.mkdir(parents=True, exist_ok=True)
        with open(run_dir / "04_verdict.json", "w") as f:
            json.dump(verdict, f, indent=2)

        print_final_report(cfg, optimize_summary, enrich_summary, roi_summary, verdict, run_dir)

        print(f"\n  Artifacts saved in: {run_dir}/")
    elif stages != "full":
        print(f"\n  Stage '{stages}' complete. Artifacts in: {run_dir}/")


if __name__ == "__main__":
    main()
