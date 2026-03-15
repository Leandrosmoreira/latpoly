"""W3 — Signal fusion worker: polls SharedState and produces normalized ticks.

Each indicator is computed by a small, pure function that returns
Optional[float] — None means "data not available yet", which is
semantically different from 0.0 and must be preserved in the output.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from latpoly.config import Config
from latpoly.shared_state import SharedState

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Indicator functions — pure, no side effects, None-safe
# ---------------------------------------------------------------------------


def compute_mid(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """Mid price from bid/ask. None if either side missing."""
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def compute_spread(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """Spread from bid/ask. None if either side missing."""
    if bid is None or ask is None:
        return None
    return ask - bid


def compute_prob_implied(
    mid_yes: Optional[float], mid_no: Optional[float]
) -> Optional[float]:
    """Implied probability of YES outcome.

    prob_implied = mid_yes / (mid_yes + mid_no)
    Returns None if inputs invalid or denominator <= 0.
    """
    if mid_yes is None or mid_no is None:
        return None
    denom = mid_yes + mid_no
    if denom <= 0.0:
        return None
    return mid_yes / denom


def compute_distance_to_strike(
    mid_binance: Optional[float], strike: float
) -> Optional[float]:
    """Distance from current BTC price to market strike.

    Positive = above strike, negative = below strike.
    Returns None if mid_binance unavailable or strike not set.
    """
    if mid_binance is None or strike == 0.0:
        return None
    return mid_binance - strike


def compute_time_to_expiry_ms(end_ts_s: float, now_s: float) -> Optional[float]:
    """Milliseconds until market expiry. Clamped to 0 if expired.

    Returns None if end_ts_s not set.
    """
    if end_ts_s == 0.0:
        return None
    return max(0.0, (end_ts_s - now_s) * 1000.0)


def compute_staleness_ms(
    ts_local_recv_ns: int, now_ns: int
) -> Optional[float]:
    """Age in ms since last update from a source.

    Returns None if source never received data (ts == 0).
    """
    if ts_local_recv_ns == 0:
        return None
    return (now_ns - ts_local_recv_ns) / 1e6


def compute_delta_price(
    prob_implied: Optional[float],
    theoretical_prob: Optional[float] = None,
) -> Optional[float]:
    """Divergence between observed implied prob and theoretical fair value.

    delta_price = prob_implied - theoretical_prob
    Returns None until theoretical_prob is available (future phases).
    """
    if prob_implied is None or theoretical_prob is None:
        return None
    return prob_implied - theoretical_prob


def compute_recv_delta_ms(
    bn_recv_ns: int, pm_recv_ns: int
) -> Optional[float]:
    """Time gap between last Binance and Polymarket receives.

    Positive = Polymarket arrived later than Binance.
    Returns None if either source has no data.
    """
    if bn_recv_ns == 0 or pm_recv_ns == 0:
        return None
    return (pm_recv_ns - bn_recv_ns) / 1e6


# ---------------------------------------------------------------------------
# Tick builder — assembles all indicators into the output dict
# ---------------------------------------------------------------------------


def build_normalized_tick(state: SharedState) -> dict:
    """Build the normalized tick from SharedState using indicator functions.

    All Optional[float] fields are kept as None in the dict (not 0.0)
    so downstream analysis can distinguish "no data" from "value is zero".
    """
    bn = state.binance
    pm = state.polymarket
    mkt = pm.market
    now_ns = time.time_ns()
    now_s = now_ns / 1e9

    # Core prices (computed here from raw bid/ask, not relying on W2 pre-computation)
    mid_binance = compute_mid(bn.best_bid, bn.best_ask)
    mid_yes = compute_mid(pm.yes_best_bid, pm.yes_best_ask)
    mid_no = compute_mid(pm.no_best_bid, pm.no_best_ask)

    # Spreads
    spread_yes = compute_spread(pm.yes_best_bid, pm.yes_best_ask)
    spread_no = compute_spread(pm.no_best_bid, pm.no_best_ask)

    # Indicators
    prob_implied = compute_prob_implied(mid_yes, mid_no)
    distance_to_strike = compute_distance_to_strike(mid_binance, mkt.strike)
    time_to_expiry_ms = compute_time_to_expiry_ms(mkt.end_ts_s, now_s)

    # Staleness
    age_binance_ms = compute_staleness_ms(bn.ts_local_recv_ns, now_ns)
    age_poly_ms = compute_staleness_ms(pm.ts_local_recv_ns, now_ns)
    recv_delta_ms = compute_recv_delta_ms(bn.ts_local_recv_ns, pm.ts_local_recv_ns)

    # Delta price (None until theoretical_prob is available in future phases)
    delta_price = compute_delta_price(prob_implied, theoretical_prob=None)

    def _r(v: Optional[float], digits: int = 2) -> Optional[float]:
        """Round if not None."""
        return round(v, digits) if v is not None else None

    return {
        # Timestamps
        "ts_ns": now_ns,
        "ts_wall": now_s,
        # Market identity
        "condition_id": mkt.condition_id,
        "strike": mkt.strike,
        "end_ts_ms": int(mkt.end_ts_s * 1000) if mkt.end_ts_s else 0,
        # Binance
        "mid_binance": _r(mid_binance, 2),
        "bn_best_bid": bn.best_bid,
        "bn_best_ask": bn.best_ask,
        "bn_last_price": bn.last_trade_price,
        "bn_last_qty": bn.last_trade_qty,
        # Polymarket YES
        "pm_yes_best_bid": pm.yes_best_bid,
        "pm_yes_best_ask": pm.yes_best_ask,
        "mid_yes": _r(mid_yes, 6),
        # Polymarket NO
        "pm_no_best_bid": pm.no_best_bid,
        "pm_no_best_ask": pm.no_best_ask,
        "mid_no": _r(mid_no, 6),
        # Spreads
        "spread_yes": _r(spread_yes, 6),
        "spread_no": _r(spread_no, 6),
        # Indicators
        "prob_implied": _r(prob_implied, 6),
        "distance_to_strike": _r(distance_to_strike, 2),
        "time_to_expiry_ms": _r(time_to_expiry_ms, 1),
        # Staleness
        "age_binance_ms": _r(age_binance_ms, 2),
        "age_poly_ms": _r(age_poly_ms, 2),
        "recv_delta_ms": _r(recv_delta_ms, 2),
        # Future: divergence
        "delta_price": _r(delta_price, 6),
    }


# ---------------------------------------------------------------------------
# Worker coroutine
# ---------------------------------------------------------------------------


async def signal_worker(
    cfg: Config,
    state: SharedState,
    out_queue: asyncio.Queue[dict],
) -> None:
    """Poll shared state at fixed interval and produce normalized ticks."""
    interval = cfg.signal_interval
    log.info("Signal worker starting (interval=%.2fs)", interval)

    # Wait for readiness
    while not state.shutdown.is_set() and not state.ready:
        await asyncio.sleep(0.5)
        if state.binance_ready and not state.polymarket_ready:
            log.debug("Waiting for Polymarket data...")
        elif state.polymarket_ready and not state.binance_ready:
            log.debug("Waiting for Binance data...")

    if state.shutdown.is_set():
        return

    log.info("Signal worker ready — both sources have data")

    try:
        while not state.shutdown.is_set():
            t0 = asyncio.get_event_loop().time()

            tick = build_normalized_tick(state)

            # Enqueue with drop-oldest if full
            try:
                out_queue.put_nowait(tick)
            except asyncio.QueueFull:
                try:
                    out_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                out_queue.put_nowait(tick)

            state.signal_tick_count += 1

            elapsed = asyncio.get_event_loop().time() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    except asyncio.CancelledError:
        log.info("Signal worker cancelled")
