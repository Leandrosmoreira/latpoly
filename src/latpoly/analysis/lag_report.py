#!/usr/bin/env python3
"""Lag & edge analysis report for latpoly normalized ticks.

Reads JSONL files produced by the writer and generates a report showing:
- Distribution of ret_1s, ret_3s, ret_5s, price_velocity
- Distribution of bn_move_since_poly (lag_edge)
- Threshold counts: abs(lag_edge) > 10, > 20
- Analysis by time-to-expiry buckets
- All stats with and without low-liquidity filter

Usage:
    python -m latpoly.analysis.lag_report data/sessions/*.jsonl
    python -m latpoly.analysis.lag_report data/daily/2026-03-16_merged.jsonl
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional


def _percentile(sorted_vals: list[float], p: float) -> float:
    """Compute percentile from pre-sorted list."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def _stats(values: list[float]) -> dict:
    """Compute distribution stats for a list of floats."""
    if not values:
        return {"count": 0, "mean": None, "median": None,
                "p75": None, "p90": None, "p95": None,
                "min": None, "max": None}
    s = sorted(values)
    n = len(s)
    return {
        "count": n,
        "mean": round(sum(s) / n, 4),
        "median": round(_percentile(s, 50), 4),
        "p75": round(_percentile(s, 75), 4),
        "p90": round(_percentile(s, 90), 4),
        "p95": round(_percentile(s, 95), 4),
        "min": round(s[0], 4),
        "max": round(s[-1], 4),
    }


def _print_stats(name: str, st: dict) -> None:
    """Pretty print one stats block."""
    if st["count"] == 0:
        print(f"  {name}: no data")
        return
    print(f"  {name} (n={st['count']})")
    print(f"    mean={st['mean']:.4f}  median={st['median']:.4f}"
          f"  p75={st['p75']:.4f}  p90={st['p90']:.4f}  p95={st['p95']:.4f}")
    print(f"    min={st['min']:.4f}  max={st['max']:.4f}")


# TTX bucket boundaries in seconds
_TTX_BUCKETS = [
    ("> 300s", 300, float("inf")),
    ("300-120s", 120, 300),
    ("120-60s", 60, 120),
    ("< 60s", 0, 60),
]


def load_ticks(paths: list[str]) -> list[dict]:
    """Load ticks from one or more JSONL files."""
    ticks = []
    for p in paths:
        path = Path(p)
        if not path.exists():
            print(f"WARNING: {p} not found, skipping")
            continue
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ticks.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return ticks


def _extract_float(tick: dict, key: str) -> Optional[float]:
    """Safely extract a float field from tick."""
    v = tick.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def run_report(ticks: list[dict]) -> None:
    """Generate and print the full lag/edge report."""
    if not ticks:
        print("No ticks to analyze.")
        return

    total = len(ticks)
    print(f"\n{'='*70}")
    print(f"  LATPOLY LAG & EDGE REPORT")
    print(f"  Total ticks: {total}")
    print(f"{'='*70}\n")

    # Separate into liquid and all
    liquid_ticks = [t for t in ticks if not t.get("low_liquidity", False)]
    low_liq_ticks = [t for t in ticks if t.get("low_liquidity", False)]

    print(f"  Ticks with real liquidity: {len(liquid_ticks)} ({100*len(liquid_ticks)/total:.1f}%)")
    print(f"  Ticks low liquidity (spread>=0.90): {len(low_liq_ticks)} ({100*len(low_liq_ticks)/total:.1f}%)")
    print()

    # --- Section 1: Momentum distribution ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Momentum Metrics ---")
        for field in ("ret_1s", "ret_3s", "ret_5s", "price_velocity"):
            vals = [v for t in dataset if (v := _extract_float(t, field)) is not None]
            _print_stats(field, _stats(vals))
        print()

    # --- Section 2: Lag edge distribution ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Lag Edge (bn_move_since_poly) ---")
        vals = [v for t in dataset if (v := _extract_float(t, "bn_move_since_poly")) is not None]
        abs_vals = [abs(v) for v in vals]
        _print_stats("bn_move_since_poly", _stats(vals))
        _print_stats("abs(bn_move_since_poly)", _stats(abs_vals))
        print()

    # --- Section 3: Threshold analysis ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Threshold Analysis ---")
        lag_edges = [(t, v) for t in dataset if (v := _extract_float(t, "bn_move_since_poly")) is not None]

        for threshold in (10, 20, 50):
            hits = [(t, v) for t, v in lag_edges if abs(v) > threshold]
            n_hits = len(hits)
            pct = 100 * n_hits / len(lag_edges) if lag_edges else 0
            print(f"  abs(lag_edge) > {threshold}: {n_hits} ticks ({pct:.2f}%)")

            if hits:
                ttx_vals = [v for t, _ in hits if (v := _extract_float(t, "time_to_expiry_ms")) is not None]
                spread_vals = [v for t, _ in hits if (v := _extract_float(t, "spread_yes")) is not None]
                avg_ttx = sum(ttx_vals) / len(ttx_vals) / 1000 if ttx_vals else 0
                avg_spread = sum(spread_vals) / len(spread_vals) if spread_vals else 0
                print(f"    avg TTX: {avg_ttx:.1f}s  avg spread_yes: {avg_spread:.4f}")
        print()

    # --- Section 4: Bucket analysis by time to expiry ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Analysis by Time-to-Expiry ---")
        for bucket_name, lo, hi in _TTX_BUCKETS:
            # Filter ticks in this TTX bucket (convert ms to s)
            bucket = [
                t for t in dataset
                if (ttx := _extract_float(t, "time_to_expiry_ms")) is not None
                and lo * 1000 <= ttx < hi * 1000
            ]
            if not bucket:
                print(f"  [{bucket_name}] no ticks")
                continue

            lag_vals = [v for t in bucket if (v := _extract_float(t, "bn_move_since_poly")) is not None]
            abs_lag = [abs(v) for v in lag_vals]

            st = _stats(abs_lag)
            gt10 = sum(1 for v in abs_lag if v > 10)
            gt20 = sum(1 for v in abs_lag if v > 20)

            print(f"  [{bucket_name}] n={len(bucket)}")
            if st["count"] > 0:
                print(f"    lag_edge: mean={st['mean']:.2f}  p95={st['p95']:.2f}  max={st['max']:.2f}")
                print(f"    |lag|>10: {gt10}  |lag|>20: {gt20}")
            else:
                print(f"    lag_edge: no data")
        print()

    # --- Section 5: Phase 2 Z-Score distribution ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Phase 2 Z-Scores ---")
        for field in ("zscore_bn_move", "zscore_ret1s", "edge_score"):
            vals = [v for t in dataset if (v := _extract_float(t, field)) is not None]
            _print_stats(field, _stats(vals))

        # Z-score threshold counts
        zs_vals = [v for t in dataset if (v := _extract_float(t, "zscore_bn_move")) is not None]
        for thresh in (1.5, 2.0, 3.0):
            hits = sum(1 for v in zs_vals if abs(v) > thresh)
            pct = 100 * hits / len(zs_vals) if zs_vals else 0
            print(f"  |zscore_bn_move| > {thresh}: {hits} ({pct:.2f}%)")

        # Edge score threshold counts
        edge_vals = [v for t in dataset if (v := _extract_float(t, "edge_score")) is not None]
        for thresh in (10, 25, 50):
            hits = sum(1 for v in edge_vals if abs(v) > thresh)
            pct = 100 * hits / len(edge_vals) if edge_vals else 0
            print(f"  |edge_score| > {thresh}: {hits} ({pct:.2f}%)")
        print()

    # --- Section 6: Poly reaction time ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Poly Reaction Time ---")
        vals = [v for t in dataset if (v := _extract_float(t, "poly_reaction_ms")) is not None]
        _print_stats("poly_reaction_ms", _stats(vals))
        print()

    # --- Section 7: Edge by time bucket ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Edge Score by Time Bucket ---")
        buckets: dict[str, list[float]] = defaultdict(list)
        for t in dataset:
            tb = t.get("time_bucket")
            es = _extract_float(t, "edge_score")
            if tb and es is not None:
                buckets[tb].append(es)
        for bname in [">600s", "600-300s", "300-120s", "120-60s", "<60s"]:
            vals = buckets.get(bname, [])
            abs_vals = [abs(v) for v in vals]
            st = _stats(abs_vals)
            if st["count"] > 0:
                print(f"  [{bname}] n={st['count']}  mean={st['mean']:.2f}  p95={st['p95']:.2f}  max={st['max']:.2f}")
            else:
                print(f"  [{bname}] no data")
        print()

    # --- Section 8: Book depth analysis ---
    for side in ("yes", "no"):
        for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
            print(f"--- {label}: Book Depth ({side.upper()} side) ---")
            for field in (f"{side}_depth_ask_total", f"{side}_depth_bid_total",
                          f"{side}_depth_ask_levels", f"{side}_depth_bid_levels"):
                vals = [v for t in dataset if (v := _extract_float(t, field)) is not None]
                _print_stats(field, _stats(vals))

            for field in (f"{side}_slippage_ask_100", f"{side}_slippage_ask_500",
                          f"{side}_slippage_bid_100", f"{side}_slippage_bid_500"):
                vals = [v for t in dataset if (v := _extract_float(t, field)) is not None]
                _print_stats(field, _stats(vals))

            # How often can we fill 100/500 contracts?
            total_d = len(dataset)
            for target in (100, 500):
                fillable = sum(1 for t in dataset if t.get(f"{side}_vwap_ask_{target}") is not None)
                pct = 100 * fillable / total_d if total_d else 0
                print(f"  Can fill {target} contracts (ask): {fillable}/{total_d} ({pct:.1f}%)")
            print()

    # --- Section 9: Depth by edge size ---
    for label, dataset in [("ALL TICKS", ticks), ("LIQUID ONLY", liquid_ticks)]:
        print(f"--- {label}: Depth when Edge > Threshold ---")
        for threshold in (10, 20, 50):
            edge_ticks = [
                t for t in dataset
                if (v := _extract_float(t, "bn_move_since_poly")) is not None and abs(v) > threshold
            ]
            if not edge_ticks:
                print(f"  |edge| > {threshold}: no ticks")
                continue
            ask_totals = [v for t in edge_ticks if (v := _extract_float(t, "yes_depth_ask_total")) is not None]
            slip_100 = [v for t in edge_ticks if (v := _extract_float(t, "yes_slippage_ask_100")) is not None]
            st_depth = _stats(ask_totals)
            st_slip = _stats(slip_100)
            fillable_100 = sum(1 for t in edge_ticks if t.get("yes_vwap_ask_100") is not None)
            print(f"  |edge| > {threshold}: n={len(edge_ticks)}")
            if st_depth["count"] > 0:
                print(f"    yes_depth_ask: mean={st_depth['mean']:.0f}  median={st_depth['median']:.0f}  p25={_percentile(sorted(ask_totals), 25):.0f}")
            if st_slip["count"] > 0:
                print(f"    yes_slippage_100: mean={st_slip['mean']:.6f}  p95={st_slip['p95']:.6f}")
            print(f"    can fill 100: {fillable_100}/{len(edge_ticks)} ({100*fillable_100/len(edge_ticks):.1f}%)")
        print()

    # --- Section 10: Quick sample of recent data ---
    print(f"--- Sample: Last 3 ticks ---")
    for t in ticks[-3:]:
        mid_bn = t.get("mid_binance", "?")
        mid_y = t.get("mid_yes", "?")
        sp_y = t.get("spread_yes", "?")
        r1 = t.get("ret_1s", "?")
        lag = t.get("bn_move_since_poly", "?")
        zs = t.get("zscore_bn_move", "?")
        es = t.get("edge_score", "?")
        tb = t.get("time_bucket", "?")
        ll = t.get("low_liquidity", "?")
        ttx = t.get("time_to_expiry_ms")
        ttx_s = f"{ttx/1000:.0f}s" if ttx else "?"
        print(f"  bn={mid_bn} yes={mid_y} lag={lag} zs={zs} edge={es} bucket={tb} liq={not ll} ttx={ttx_s}")

    print(f"\n{'='*70}")
    print(f"  Report complete.")
    print(f"{'='*70}\n")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m latpoly.analysis.lag_report <file.jsonl> [file2.jsonl ...]")
        sys.exit(1)

    ticks = load_ticks(sys.argv[1:])
    run_report(ticks)


if __name__ == "__main__":
    main()
