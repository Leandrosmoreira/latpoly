#!/usr/bin/env python3
"""Enriched log pipeline for latpoly — execution-aware tick enrichment.

Reads raw JSONL tick logs and produces enriched JSONL with:
- Forward-looking price features (200ms, 500ms, 1000ms ahead)
- Simulated execution metrics (fill price, slippage, edge decay)
- Velocity/competition indicators
- Rule-based cluster classification (A-F)
- Execution risk score (0-1)

Streams data through a sliding window — constant memory, handles multi-GB files.

Usage:
    python -m latpoly.analysis.enrich data/daily/*.jsonl --output data/enriched/
    python -m latpoly.analysis.enrich data/daily/*.jsonl --output data/enriched/ --slot=btc-15m
    python -m latpoly.analysis.enrich data/daily/*.jsonl --output data/enriched/ --latency-profile=fast
"""

from __future__ import annotations

import json
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# At ~5 ticks/sec: 1 tick ~ 200ms, 3 ticks ~ 500ms, 5 ticks ~ 1000ms
LOOKAHEAD_HORIZONS = [1, 3, 5]
LOOKAHEAD_MAX = max(LOOKAHEAD_HORIZONS)
WINDOW_SIZE = LOOKAHEAD_MAX + 16  # deque buffer

LATENCY_PROFILES = {
    "fast":   {"latency_ms": 100, "lookahead_ticks": 1},
    "medium": {"latency_ms": 250, "lookahead_ticks": 2},
    "slow":   {"latency_ms": 500, "lookahead_ticks": 3},
}

# Cluster thresholds
_BN_MOVE_STRONG = 15.0
_BN_MOVE_MODERATE = 10.0
_BN_MOVE_WEAK = 3.0
_EDGE_REMAINING_GOOD = 5.0
_SPREAD_TIGHTEN_THRESHOLD = -0.005

# Execution risk weights
_W_EDGE_DECAY = 0.30
_W_SIGNAL_FLIP = 0.25
_W_SLIPPAGE = 0.20
_W_COMPETITION = 0.15
_W_LIQUIDITY = 0.10
_SLIPPAGE_NORM = 0.03
_SPREAD_TIGHTEN_NORM = 0.02

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ef(tick: dict, key: str) -> Optional[float]:
    """Extract float from tick, returning None if missing or non-numeric."""
    v = tick.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


# ---------------------------------------------------------------------------
# Forward Features
# ---------------------------------------------------------------------------


def compute_forward_features(tick: dict, window: list[dict]) -> dict:
    """Compute forward-looking features at horizons 1t, 3t, 5t.

    Args:
        tick: Current tick dict.
        window: List of future ticks (index 0 = next tick).

    Returns:
        Dict with fwd_{h}t_* fields.
    """
    out: dict = {}
    cid = tick.get("condition_id")
    mid_bn = _ef(tick, "mid_binance")
    mid_yes = _ef(tick, "mid_yes")
    mid_no = _ef(tick, "mid_no")
    spread_yes = _ef(tick, "spread_yes")
    bn_move = _ef(tick, "bn_move_since_poly")

    for h in LOOKAHEAD_HORIZONS:
        prefix = f"fwd_{h}t"
        idx = h - 1  # window[0] = 1 tick ahead

        if idx < len(window) and window[idx].get("condition_id") == cid:
            ft = window[idx]
            ft_mid_bn = _ef(ft, "mid_binance")
            ft_mid_yes = _ef(ft, "mid_yes")
            ft_mid_no = _ef(ft, "mid_no")
            ft_spread_yes = _ef(ft, "spread_yes")
            ft_bn_move = _ef(ft, "bn_move_since_poly")

            out[f"{prefix}_mid_bn_delta"] = (
                round(ft_mid_bn - mid_bn, 4) if ft_mid_bn is not None and mid_bn is not None else None
            )
            out[f"{prefix}_mid_yes_delta"] = (
                round(ft_mid_yes - mid_yes, 6) if ft_mid_yes is not None and mid_yes is not None else None
            )
            out[f"{prefix}_mid_no_delta"] = (
                round(ft_mid_no - mid_no, 6) if ft_mid_no is not None and mid_no is not None else None
            )
            out[f"{prefix}_spread_yes"] = (
                round(ft_spread_yes, 4) if ft_spread_yes is not None else None
            )
            out[f"{prefix}_edge_decay"] = (
                round(abs(ft_bn_move) - abs(bn_move), 4)
                if ft_bn_move is not None and bn_move is not None
                else None
            )
        else:
            out[f"{prefix}_mid_bn_delta"] = None
            out[f"{prefix}_mid_yes_delta"] = None
            out[f"{prefix}_mid_no_delta"] = None
            out[f"{prefix}_spread_yes"] = None
            out[f"{prefix}_edge_decay"] = None

    return out


# ---------------------------------------------------------------------------
# Execution Simulation
# ---------------------------------------------------------------------------


def compute_execution_sim(tick: dict, window: list[dict], profile: dict) -> dict:
    """Simulate what happens if bot's order arrives after latency delay.

    Uses the latency profile's lookahead_ticks to pick the execution tick.
    """
    la = profile["lookahead_ticks"]
    cid = tick.get("condition_id")
    idx = la - 1

    if idx < len(window) and window[idx].get("condition_id") == cid:
        et = window[idx]
        # Fill price: VWAP ask 100 if available, else best ask
        fill_yes = _ef(et, "yes_vwap_ask_100") or _ef(et, "pm_yes_best_ask")
        fill_no = _ef(et, "no_vwap_ask_100") or _ef(et, "pm_no_best_ask")

        cur_ask_yes = _ef(tick, "pm_yes_best_ask")
        cur_ask_no = _ef(tick, "pm_no_best_ask")

        slip_yes = round(fill_yes - cur_ask_yes, 6) if fill_yes and cur_ask_yes else None
        slip_no = round(fill_no - cur_ask_no, 6) if fill_no and cur_ask_no else None

        edge_remaining = _ef(et, "bn_move_since_poly")
        bn_move_now = _ef(tick, "bn_move_since_poly")

        # Signal survived if same sign and still significant
        survived = None
        if edge_remaining is not None and bn_move_now is not None and bn_move_now != 0:
            same_sign = (edge_remaining > 0) == (bn_move_now > 0)
            survived = same_sign and abs(edge_remaining) > 1.0

        return {
            "exec_fill_price_yes": round(fill_yes, 4) if fill_yes else None,
            "exec_fill_price_no": round(fill_no, 4) if fill_no else None,
            "exec_slippage_yes": slip_yes,
            "exec_slippage_no": slip_no,
            "exec_edge_remaining": round(edge_remaining, 4) if edge_remaining is not None else None,
            "exec_signal_survived": survived,
        }

    return {
        "exec_fill_price_yes": None,
        "exec_fill_price_no": None,
        "exec_slippage_yes": None,
        "exec_slippage_no": None,
        "exec_edge_remaining": None,
        "exec_signal_survived": None,
    }


# ---------------------------------------------------------------------------
# Velocity / Competition Indicators
# ---------------------------------------------------------------------------


def compute_velocity_indicators(tick: dict, fwd: dict, exec_sim: dict,
                                window: list[dict]) -> dict:
    """Compute price acceleration, spread tightening, depth drain."""
    ret_1s = _ef(tick, "ret_1s")
    fwd_1t_bn = fwd.get("fwd_1t_mid_bn_delta")

    # Price acceleration: is BTC speeding up or slowing?
    price_accel = None
    if fwd_1t_bn is not None and ret_1s is not None:
        price_accel = round(fwd_1t_bn - ret_1s, 6)

    # Spread tightening: negative = competition arrived
    spread_yes_now = _ef(tick, "spread_yes")
    fwd_spread = fwd.get("fwd_1t_spread_yes")
    spread_tightening = None
    if fwd_spread is not None and spread_yes_now is not None:
        spread_tightening = round(fwd_spread - spread_yes_now, 6)

    # Depth drain: how much ask liquidity disappeared
    depth_now = _ef(tick, "yes_depth_ask_total")
    depth_drain = None
    cid = tick.get("condition_id")
    if window and window[0].get("condition_id") == cid:
        depth_future = _ef(window[0], "yes_depth_ask_total")
        if depth_now and depth_future and depth_now > 0:
            depth_drain = round((depth_future - depth_now) / depth_now, 4)

    return {
        "price_accel": price_accel,
        "spread_tightening": spread_tightening,
        "depth_drain_rate": depth_drain,
    }


# ---------------------------------------------------------------------------
# Cluster Classification (rule-based)
# ---------------------------------------------------------------------------


def classify_tick(tick: dict, exec_sim: dict) -> str:
    """Assign one of 6 clusters to the tick. First match wins.

    F: illiquid (checked first)
    A: fast_edge — strong signal that survives execution
    B: decaying_edge — signal survives but edge mostly gone
    C: crowded — signal exists but competition already acting
    D: low_edge — small signal, not worth fees
    E: noise — no meaningful signal (default)
    """
    # F — illiquid: cannot trade regardless
    if tick.get("low_liquidity") is True:
        return "F_illiquid"

    bn_move = _ef(tick, "bn_move_since_poly")
    abs_move = abs(bn_move) if bn_move is not None else 0
    survived = exec_sim.get("exec_signal_survived")
    edge_rem = exec_sim.get("exec_edge_remaining")
    abs_edge_rem = abs(edge_rem) if edge_rem is not None else 0
    spread_tight = _ef(tick, "spread_tightening") or tick.get("spread_tightening")

    # A — strong signal, survives, edge still present
    if abs_move > _BN_MOVE_STRONG and survived is True and abs_edge_rem > _EDGE_REMAINING_GOOD:
        return "A_fast_edge"

    # B — signal survives but edge mostly consumed
    if abs_move > _BN_MOVE_MODERATE and survived is True and abs_edge_rem <= _EDGE_REMAINING_GOOD:
        return "B_decaying_edge"

    # C — signal but spreads tightening (competitors)
    if abs_move > _BN_MOVE_MODERATE and spread_tight is not None:
        try:
            if float(spread_tight) < _SPREAD_TIGHTEN_THRESHOLD:
                return "C_crowded"
        except (ValueError, TypeError):
            pass

    # D — small signal
    if abs_move > _BN_MOVE_WEAK:
        return "D_low_edge"

    # E — noise
    return "E_noise"


# ---------------------------------------------------------------------------
# Execution Risk Score
# ---------------------------------------------------------------------------


def compute_execution_risk(tick: dict, exec_sim: dict, velocity: dict) -> float:
    """Weighted score from 0 (safe) to 1 (risky)."""
    bn_move = _ef(tick, "bn_move_since_poly")
    abs_move = abs(bn_move) if bn_move is not None else 0

    # 1. Edge decay risk
    edge_rem = exec_sim.get("exec_edge_remaining")
    if edge_rem is not None and abs_move > 0:
        edge_decay_risk = max(0.0, min(1.0, 1.0 - abs(edge_rem) / abs_move))
    else:
        edge_decay_risk = 0.5  # unknown → neutral

    # 2. Signal flip risk
    survived = exec_sim.get("exec_signal_survived")
    signal_flip_risk = 0.0 if survived is True else (1.0 if survived is False else 0.5)

    # 3. Slippage risk
    slip = exec_sim.get("exec_slippage_yes")
    if slip is not None:
        slippage_risk = min(1.0, abs(slip) / _SLIPPAGE_NORM)
    else:
        slippage_risk = 0.5

    # 4. Competition risk
    spread_tight = velocity.get("spread_tightening")
    if spread_tight is not None:
        competition_risk = min(1.0, max(0.0, -spread_tight) / _SPREAD_TIGHTEN_NORM)
    else:
        competition_risk = 0.5

    # 5. Liquidity risk
    if tick.get("low_liquidity") is True:
        liquidity_risk = 1.0
    else:
        depth = _ef(tick, "yes_depth_ask_total")
        if depth and depth > 0:
            liquidity_risk = min(1.0, 100.0 / depth)
        else:
            liquidity_risk = 0.8

    score = (
        _W_EDGE_DECAY * edge_decay_risk
        + _W_SIGNAL_FLIP * signal_flip_risk
        + _W_SLIPPAGE * slippage_risk
        + _W_COMPETITION * competition_risk
        + _W_LIQUIDITY * liquidity_risk
    )
    return round(score, 4)


# ---------------------------------------------------------------------------
# Enrichment Summary
# ---------------------------------------------------------------------------


@dataclass
class EnrichSummary:
    total_ticks: int = 0
    enriched_ticks: int = 0
    skipped_slot: int = 0
    cluster_counts: dict = field(default_factory=lambda: defaultdict(int))
    cluster_risk_sums: dict = field(default_factory=lambda: defaultdict(float))
    cluster_slip_sums: dict = field(default_factory=lambda: defaultdict(float))
    risk_scores: list = field(default_factory=list)
    signal_survived_count: int = 0
    signal_total_count: int = 0
    elapsed_s: float = 0.0

    def update(self, enriched: dict) -> None:
        self.enriched_ticks += 1
        cluster = enriched.get("cluster", "E_noise")
        risk = enriched.get("execution_risk_score", 0)
        slip = enriched.get("exec_slippage_yes")

        self.cluster_counts[cluster] += 1
        self.cluster_risk_sums[cluster] += risk
        if slip is not None:
            self.cluster_slip_sums[cluster] += abs(slip)

        self.risk_scores.append(risk)

        survived = enriched.get("exec_signal_survived")
        if survived is not None:
            self.signal_total_count += 1
            if survived is True:
                self.signal_survived_count += 1


# ---------------------------------------------------------------------------
# Core: enrich a single file via sliding window
# ---------------------------------------------------------------------------


def enrich_file(
    input_path: Path,
    output_path: Path,
    profile_name: str = "medium",
    slot_filter: Optional[str] = None,
    summary: Optional[EnrichSummary] = None,
) -> EnrichSummary:
    """Stream-enrich a raw JSONL file into an enriched JSONL file.

    Uses a sliding window deque for constant memory usage.
    """
    if summary is None:
        summary = EnrichSummary()

    profile = LATENCY_PROFILES[profile_name]
    window: deque = deque(maxlen=WINDOW_SIZE)
    slot_check = f'"slot_id":"{slot_filter}"' if slot_filter else None

    size_mb = input_path.stat().st_size / (1024 * 1024)
    print(f"  Processing {input_path.name} ({size_mb:.1f} MB) ...", end="", flush=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_ticks = 0
    file_enriched = 0

    with open(input_path, "r") as fin, open(output_path, "w") as fout:
        # Phase 1: fill initial window
        for line in fin:
            line = line.strip()
            if not line:
                continue

            # Fast slot pre-filter (before JSON parsing)
            if slot_check and '"slot_id"' in line and slot_check not in line:
                summary.skipped_slot += 1
                continue

            try:
                tick = json.loads(line)
            except json.JSONDecodeError:
                continue

            if slot_filter and tick.get("slot_id", slot_filter) != slot_filter:
                summary.skipped_slot += 1
                continue

            summary.total_ticks += 1
            window.append(tick)

            # Once we have enough future ticks, start enriching
            if len(window) > LOOKAHEAD_MAX:
                enriched = _enrich_tick(window[0], list(window)[1:], profile, profile_name)
                summary.update(enriched)
                fout.write(json.dumps(enriched, separators=(",", ":")) + "\n")
                file_enriched += 1
                window.popleft()

            file_ticks += 1
            if file_ticks % 100000 == 0:
                print(".", end="", flush=True)

        # Phase 2: drain remaining ticks in window
        while window:
            tick = window.popleft()
            future = list(window)
            enriched = _enrich_tick(tick, future, profile, profile_name)
            summary.update(enriched)
            fout.write(json.dumps(enriched, separators=(",", ":")) + "\n")
            file_enriched += 1

    print(f" {file_enriched:,} enriched", flush=True)
    return summary


def _enrich_tick(tick: dict, future: list[dict], profile: dict,
                 profile_name: str) -> dict:
    """Enrich a single tick with all computed fields."""
    fwd = compute_forward_features(tick, future)
    exec_sim = compute_execution_sim(tick, future, profile)
    velocity = compute_velocity_indicators(tick, fwd, exec_sim, future)

    # Merge velocity into tick temporarily for classify_tick to use spread_tightening
    tick_with_vel = {**tick, **velocity}
    cluster = classify_tick(tick_with_vel, exec_sim)
    risk = compute_execution_risk(tick, exec_sim, velocity)

    # Build enriched tick: original + all new fields
    enriched = dict(tick)
    enriched.update(fwd)
    enriched.update(exec_sim)
    enriched.update(velocity)
    enriched["cluster"] = cluster
    enriched["execution_risk_score"] = risk
    enriched["latency_profile"] = profile_name

    return enriched


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_enrich_report(summary: EnrichSummary) -> None:
    """Print formatted enrichment summary."""
    print()
    print("=" * 70)
    print("  ENRICHMENT REPORT")
    print("=" * 70)
    print(f"  Total ticks read:    {summary.total_ticks:>12,}")
    print(f"  Enriched ticks:      {summary.enriched_ticks:>12,}")
    print(f"  Skipped (slot):      {summary.skipped_slot:>12,}")
    print(f"  Elapsed:             {summary.elapsed_s:>11.1f}s")
    if summary.elapsed_s > 0:
        rate = summary.enriched_ticks / summary.elapsed_s
        print(f"  Rate:                {rate:>11,.0f} ticks/sec")

    # Cluster distribution
    print()
    print("  --- Cluster Distribution ---")
    print(f"  {'Cluster':<20s} {'Count':>8s} {'%':>6s} {'AvgRisk':>8s} {'AvgSlip':>9s}")
    print(f"  {'-'*20} {'-'*8} {'-'*6} {'-'*8} {'-'*9}")

    total = max(summary.enriched_ticks, 1)
    for cluster in sorted(summary.cluster_counts.keys()):
        cnt = summary.cluster_counts[cluster]
        pct = cnt / total * 100
        avg_risk = summary.cluster_risk_sums[cluster] / max(cnt, 1)
        avg_slip = summary.cluster_slip_sums[cluster] / max(cnt, 1)
        print(f"  {cluster:<20s} {cnt:>8,} {pct:>5.1f}% {avg_risk:>8.4f} {avg_slip:>9.6f}")

    # Risk score distribution
    if summary.risk_scores:
        s = sorted(summary.risk_scores)
        print()
        print("  --- Execution Risk Score Distribution ---")
        print(f"  p50 = {_percentile(s, 50):.4f}")
        print(f"  p75 = {_percentile(s, 75):.4f}")
        print(f"  p90 = {_percentile(s, 90):.4f}")
        print(f"  p95 = {_percentile(s, 95):.4f}")
        print(f"  min = {s[0]:.4f}  max = {s[-1]:.4f}")

    # Signal survival
    if summary.signal_total_count > 0:
        surv_rate = summary.signal_survived_count / summary.signal_total_count * 100
        print()
        print(f"  --- Signal Survival ---")
        print(f"  Survived: {summary.signal_survived_count:,} / {summary.signal_total_count:,} ({surv_rate:.1f}%)")

    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI: python -m latpoly.analysis.enrich <files> --output <dir> [options]"""
    args = sys.argv[1:]

    if not args or "--help" in args or "-h" in args:
        print(__doc__)
        sys.exit(0)

    # Parse arguments
    input_files: list[str] = []
    output_dir: str = "data/enriched"
    profile_name: str = "medium"
    slot_filter: Optional[str] = None

    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--output="):
            output_dir = a.split("=", 1)[1]
        elif a == "--output" and i + 1 < len(args):
            i += 1
            output_dir = args[i]
        elif a.startswith("--latency-profile="):
            profile_name = a.split("=", 1)[1]
        elif a.startswith("--slot="):
            slot_filter = a.split("=", 1)[1]
        elif not a.startswith("--"):
            input_files.append(a)
        i += 1

    if not input_files:
        print("ERROR: no input files specified")
        sys.exit(1)

    if profile_name not in LATENCY_PROFILES:
        print(f"ERROR: unknown profile '{profile_name}'. Options: {list(LATENCY_PROFILES.keys())}")
        sys.exit(1)

    # Resolve paths
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Enriching {len(input_files)} file(s) with profile={profile_name}"
          + (f", slot={slot_filter}" if slot_filter else ""))

    summary = EnrichSummary()
    t0 = time.monotonic()

    for fpath in input_files:
        inp = Path(fpath)
        if not inp.exists():
            print(f"  WARNING: {fpath} not found, skipping")
            continue

        # Output filename: same stem + _enriched
        out_name = inp.stem.replace("_merged", "") + "_enriched.jsonl"
        out_path = out_dir / out_name

        enrich_file(inp, out_path, profile_name, slot_filter, summary)

    summary.elapsed_s = time.monotonic() - t0
    print_enrich_report(summary)


if __name__ == "__main__":
    main()
