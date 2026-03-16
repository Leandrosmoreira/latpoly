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

    # --- Section 5: Quick sample of recent data ---
    print(f"--- Sample: Last 3 ticks ---")
    for t in ticks[-3:]:
        mid_bn = t.get("mid_binance", "?")
        mid_y = t.get("mid_yes", "?")
        sp_y = t.get("spread_yes", "?")
        r1 = t.get("ret_1s", "?")
        r3 = t.get("ret_3s", "?")
        lag = t.get("bn_move_since_poly", "?")
        ll = t.get("low_liquidity", "?")
        ttx = t.get("time_to_expiry_ms")
        ttx_s = f"{ttx/1000:.0f}s" if ttx else "?"
        print(f"  bn={mid_bn} yes={mid_y} sp={sp_y} r1s={r1} r3s={r3} lag={lag} liq={not ll} ttx={ttx_s}")

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
