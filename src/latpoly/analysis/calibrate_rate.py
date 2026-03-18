#!/usr/bin/env python3
"""Empirical calibration of btc_to_pm_rate.

Measures how much Polymarket mid actually moves per $1 BTC lag,
broken down by mid range and BTC move magnitude.

Usage:
    python -m latpoly.analysis.calibrate_rate data/daily/*.jsonl
"""

from __future__ import annotations

import sys
from collections import defaultdict

from latpoly.analysis.lag_report import load_ticks


# ---------------------------------------------------------------------------
# Calibration logic
# ---------------------------------------------------------------------------

# How many ticks ahead to look for PM response (at ~5 ticks/s = 1-4 seconds)
LOOKAHEAD_TICKS = [5, 10, 15, 20]

# Mid buckets
MID_BUCKETS = [
    (0.05, 0.20, "0.05-0.20"),
    (0.20, 0.35, "0.20-0.35"),
    (0.35, 0.50, "0.35-0.50"),
    (0.50, 0.65, "0.50-0.65"),
    (0.65, 0.80, "0.65-0.80"),
    (0.80, 0.95, "0.80-0.95"),
]

# BTC move magnitude buckets
BN_MOVE_BUCKETS = [
    (1.0, 3.0, "$1-3"),
    (3.0, 6.0, "$3-6"),
    (6.0, 10.0, "$6-10"),
    (10.0, 20.0, "$10-20"),
    (20.0, 50.0, "$20-50"),
    (50.0, 999.0, "$50+"),
]


def _get_mid_bucket(mid: float) -> str | None:
    for lo, hi, name in MID_BUCKETS:
        if lo <= mid < hi:
            return name
    return None


def _get_bn_bucket(bn_move_abs: float) -> str | None:
    for lo, hi, name in BN_MOVE_BUCKETS:
        if lo <= bn_move_abs < hi:
            return name
    return None


def calibrate(ticks: list[dict]) -> dict:
    """Measure actual PM response per $1 BTC move.

    For each tick with a significant z-score signal (|zscore| >= 1.5):
    - Record current mid_yes and bn_move_since_poly
    - Look ahead 5/10/15/20 ticks to see how mid_yes changed
    - Compute: pm_response = |delta_mid| / |bn_move|
    - Normalize by probability sensitivity: raw_rate = pm_response / (4*mid*(1-mid))
    - Bucket by mid range and BTC move magnitude
    """
    n = len(ticks)

    # Storage: {(mid_bucket, bn_bucket): [raw_rate_values]}
    raw_rates: dict[tuple[str, str], list[float]] = defaultdict(list)
    pm_responses: dict[tuple[str, str], list[float]] = defaultdict(list)

    # Also track per-lookahead for optimal window
    by_lookahead: dict[int, list[float]] = defaultdict(list)

    signals_found = 0
    signals_measured = 0

    for i in range(n - max(LOOKAHEAD_TICKS)):
        tick = ticks[i]

        # Need z-score signal and mid data
        zscore = tick.get("zscore_bn_move")
        bn_move = tick.get("bn_move_since_poly")
        mid = tick.get("mid_yes")
        cid = tick.get("condition_id")

        if zscore is None or bn_move is None or mid is None or cid is None:
            continue
        if abs(zscore) < 1.5:
            continue
        if abs(bn_move) < 1.0:
            continue

        bn_move_abs = abs(bn_move)
        mid_bucket = _get_mid_bucket(mid)
        bn_bucket = _get_bn_bucket(bn_move_abs)

        if mid_bucket is None or bn_bucket is None:
            continue

        signals_found += 1

        # Probability sensitivity at this mid
        prob_sens = 4.0 * mid * (1.0 - mid)
        if prob_sens < 0.1:
            continue  # too extreme

        # Look ahead for PM response
        for la in LOOKAHEAD_TICKS:
            future_idx = i + la
            if future_idx >= n:
                break

            future_tick = ticks[future_idx]
            future_mid = future_tick.get("mid_yes")
            future_cid = future_tick.get("condition_id")

            # Must be same market
            if future_cid != cid or future_mid is None:
                continue

            # PM response: how much did mid change in the direction of bn_move?
            delta_mid = future_mid - mid

            # Did PM move in the expected direction?
            if bn_move > 0:
                # BTC up -> YES should increase
                pm_response = delta_mid
            else:
                # BTC down -> YES should decrease -> NO increases
                pm_response = -delta_mid

            # Raw rate: response per $1 BTC, normalized by sensitivity
            if bn_move_abs > 0:
                response_per_dollar = pm_response / bn_move_abs
                raw_rate = response_per_dollar / prob_sens if prob_sens > 0 else 0

                key = (mid_bucket, bn_bucket)
                raw_rates[key].append(raw_rate)
                pm_responses[key].append(response_per_dollar)
                by_lookahead[la].append(response_per_dollar)

                if la == 10:  # primary lookahead
                    signals_measured += 1

    return {
        "raw_rates": raw_rates,
        "pm_responses": pm_responses,
        "by_lookahead": by_lookahead,
        "signals_found": signals_found,
        "signals_measured": signals_measured,
    }


def print_calibration_report(result: dict) -> None:
    """Print formatted calibration results."""
    print(f"\n{'='*80}")
    print(f"  BTC-TO-PM RATE CALIBRATION REPORT")
    print(f"{'='*80}\n")

    print(f"  Signals with |zscore|>=1.5 and |bn_move|>=$1: {result['signals_found']:,}")
    print(f"  Signals measured (10-tick lookahead): {result['signals_measured']:,}")
    print()

    # --- Optimal lookahead ---
    print("  --- PM Response by Lookahead Window ---")
    for la in sorted(result["by_lookahead"].keys()):
        values = result["by_lookahead"][la]
        if not values:
            continue
        avg = sum(values) / len(values)
        median = sorted(values)[len(values) // 2]
        positive = sum(1 for v in values if v > 0)
        print(f"  {la:2d} ticks (~{la/5:.1f}s):  n={len(values):6d}  "
              f"avg={avg:.6f}  med={median:.6f}  "
              f"positive={positive/len(values)*100:.0f}%")
    print()

    # --- Rate by mid bucket ---
    print("  --- PM Response per $1 BTC by Mid Range ---")
    print(f"  {'Mid Range':12s}  {'n':>6s}  {'avg_response':>12s}  {'raw_rate':>10s}  {'suggested_rate':>14s}")
    print(f"  {'-'*12}  {'-'*6}  {'-'*12}  {'-'*10}  {'-'*14}")

    overall_rates = []
    for lo, hi, name in MID_BUCKETS:
        # Aggregate across all BTC magnitude buckets for this mid range
        all_responses = []
        all_raw = []
        for (mb, bb), vals in result["pm_responses"].items():
            if mb == name:
                all_responses.extend(vals)
        for (mb, bb), vals in result["raw_rates"].items():
            if mb == name:
                all_raw.extend(vals)

        if not all_responses:
            print(f"  {name:12s}  {'0':>6s}  {'N/A':>12s}  {'N/A':>10s}  {'N/A':>14s}")
            continue

        avg_resp = sum(all_responses) / len(all_responses)
        avg_raw = sum(all_raw) / len(all_raw) if all_raw else 0
        mid_center = (lo + hi) / 2
        prob_sens = 4.0 * mid_center * (1.0 - mid_center)
        suggested = avg_resp / prob_sens if prob_sens > 0 else 0

        overall_rates.append(suggested)
        print(f"  {name:12s}  {len(all_responses):6d}  {avg_resp:12.6f}  {avg_raw:10.6f}  {suggested:14.6f}")

    print()

    # --- Rate by BTC magnitude ---
    print("  --- PM Response per $1 BTC by BTC Move Size ---")
    print(f"  {'BTC Move':12s}  {'n':>6s}  {'avg_response':>12s}  {'% positive':>10s}")
    print(f"  {'-'*12}  {'-'*6}  {'-'*12}  {'-'*10}")

    for blo, bhi, bname in BN_MOVE_BUCKETS:
        all_responses = []
        for (mb, bb), vals in result["pm_responses"].items():
            if bb == bname:
                all_responses.extend(vals)

        if not all_responses:
            print(f"  {bname:12s}  {'0':>6s}  {'N/A':>12s}  {'N/A':>10s}")
            continue

        avg_resp = sum(all_responses) / len(all_responses)
        pct_pos = sum(1 for v in all_responses if v > 0) / len(all_responses) * 100
        print(f"  {bname:12s}  {len(all_responses):6d}  {avg_resp:12.6f}  {pct_pos:9.0f}%")

    print()

    # --- Full matrix ---
    print("  --- Full Matrix: avg response per $1 BTC ---")
    header = f"  {'':12s}"
    for _, _, bname in BN_MOVE_BUCKETS:
        header += f"  {bname:>8s}"
    print(header)

    for _, _, mname in MID_BUCKETS:
        row = f"  {mname:12s}"
        for _, _, bname in BN_MOVE_BUCKETS:
            key = (mname, bname)
            vals = result["pm_responses"].get(key, [])
            if vals:
                avg = sum(vals) / len(vals)
                row += f"  {avg:8.5f}"
            else:
                row += f"  {'---':>8s}"
        print(row)

    print()

    # --- Recommendation ---
    if overall_rates:
        recommended = sum(overall_rates) / len(overall_rates)
        print(f"  *** RECOMMENDED btc_to_pm_base_rate: {recommended:.6f} ***")
        print(f"      (current default: 0.000800)")
        print(f"      ratio: {recommended / 0.0008:.1f}x current")
        print()

        # Fee breakeven analysis
        print("  --- Fee Breakeven Analysis (1% taker) ---")
        print(f"  {'Mid':>5s}  {'Fee':>6s}  {'Breakeven BN Move':>18s}")
        for mid_test in [0.20, 0.30, 0.40, 0.50, 0.60, 0.70]:
            fee = mid_test * 0.01
            prob_sens = 4.0 * mid_test * (1.0 - mid_test)
            rate = recommended * prob_sens
            if rate > 0:
                breakeven = fee / rate
            else:
                breakeven = 999
            print(f"  {mid_test:5.2f}  ${fee:.4f}  ${breakeven:>16.1f}")

    print(f"\n{'='*80}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    args = sys.argv[1:]
    file_args = [a for a in args if not a.startswith("--")]

    if not file_args:
        print("Usage: python -m latpoly.analysis.calibrate_rate <file.jsonl> [...]")
        sys.exit(1)

    print(f"Loading ticks from {len(file_args)} file(s)...")
    ticks = load_ticks(file_args)
    print(f"Loaded {len(ticks):,} ticks")

    if not ticks:
        print("No ticks loaded.")
        sys.exit(1)

    print("Calibrating btc_to_pm_rate from empirical data...")
    result = calibrate(ticks)
    print_calibration_report(result)


if __name__ == "__main__":
    main()
