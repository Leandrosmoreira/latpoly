"""W3 — Signal fusion worker: polls SharedState and produces normalized ticks.

Each indicator is computed by a small, pure function that returns
Optional[float] — None means "data not available yet", which is
semantically different from 0.0 and must be preserved in the output.

Phase 1.1 additions:
- ret_1s, ret_3s, ret_5s: short Binance returns
- price_velocity: smoothed USD/s speed (ret_3s / 3.0)
- binance_move_since_last_poly_update: lag edge metric
- low_liquidity: flag for empty/artificial books
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Optional

from latpoly.config import Config
from latpoly.shared_state import SharedState

log = logging.getLogger(__name__)

# Max history entries: 5s at 5 ticks/s = 25, add margin
_HISTORY_MAXLEN = 50
_HISTORY_MAX_AGE_NS = 6_000_000_000  # 6s (keep slightly beyond 5s)

# Spread threshold for low liquidity detection
_LOW_LIQUIDITY_SPREAD = 0.90


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
# Phase 1.1: Price history buffer for short returns
# ---------------------------------------------------------------------------


class PriceHistory:
    """Lightweight ring buffer of (ts_ns, mid_binance) for short returns.

    Only keeps entries within _HISTORY_MAX_AGE_NS. Uses deque with maxlen
    so memory is bounded even if cleanup lags.
    """

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf: deque[tuple[int, float]] = deque(maxlen=_HISTORY_MAXLEN)

    def push(self, ts_ns: int, mid: float) -> None:
        """Add a new price point and evict stale entries."""
        self._buf.append((ts_ns, mid))
        # Evict entries older than max age from the left
        cutoff = ts_ns - _HISTORY_MAX_AGE_NS
        while self._buf and self._buf[0][0] < cutoff:
            self._buf.popleft()

    def get_price_ago(self, now_ns: int, seconds: float) -> Optional[float]:
        """Find the mid closest to `seconds` ago. None if no data."""
        if not self._buf:
            return None
        target_ns = now_ns - int(seconds * 1e9)
        # Need at least some history reaching back to target
        if self._buf[0][0] > target_ns:
            return None  # not enough history yet
        # Binary-ish search: iterate from end (newest) to find closest
        best_ts = 0
        best_mid = 0.0
        best_diff = float("inf")
        for ts, mid in self._buf:
            diff = abs(ts - target_ns)
            if diff < best_diff:
                best_diff = diff
                best_ts = ts
                best_mid = mid
        # Accept if within 500ms tolerance
        if best_diff > 500_000_000:
            return None
        return best_mid

    def clear(self) -> None:
        """Clear buffer on market rotation."""
        self._buf.clear()


def compute_return(
    current: Optional[float], historical: Optional[float]
) -> Optional[float]:
    """Simple return: current - historical. None if either missing."""
    if current is None or historical is None:
        return None
    return current - historical


def compute_velocity(ret_3s: Optional[float]) -> Optional[float]:
    """Price velocity in USD/s from 3s return (smoother than 1s)."""
    if ret_3s is None:
        return None
    return ret_3s / 3.0


# ---------------------------------------------------------------------------
# Phase 1.1: Polymarket change tracker
# ---------------------------------------------------------------------------


class PolyChangeTracker:
    """Detects real changes in Polymarket state and tracks lag edge.

    A "real change" is any alteration in best_bid/ask for YES or NO.
    When a change occurs, we snapshot the current Binance mid so we can
    measure how much Binance moved since the last Polymarket update.
    """

    __slots__ = (
        "_last_yes_bid", "_last_yes_ask",
        "_last_no_bid", "_last_no_ask",
        "last_change_ts_ns",
        "mid_binance_at_last_change",
    )

    def __init__(self) -> None:
        self._last_yes_bid: Optional[float] = None
        self._last_yes_ask: Optional[float] = None
        self._last_no_bid: Optional[float] = None
        self._last_no_ask: Optional[float] = None
        self.last_change_ts_ns: int = 0
        self.mid_binance_at_last_change: Optional[float] = None

    def check_and_update(
        self,
        yes_bid: Optional[float],
        yes_ask: Optional[float],
        no_bid: Optional[float],
        no_ask: Optional[float],
        mid_binance: Optional[float],
        now_ns: int,
    ) -> bool:
        """Check if poly state changed. If so, update tracker. Returns True on change."""
        changed = (
            yes_bid != self._last_yes_bid
            or yes_ask != self._last_yes_ask
            or no_bid != self._last_no_bid
            or no_ask != self._last_no_ask
        )
        if changed:
            self._last_yes_bid = yes_bid
            self._last_yes_ask = yes_ask
            self._last_no_bid = no_bid
            self._last_no_ask = no_ask
            self.last_change_ts_ns = now_ns
            self.mid_binance_at_last_change = mid_binance
        return changed

    def compute_binance_move(self, mid_binance: Optional[float]) -> Optional[float]:
        """How much Binance moved since the last Polymarket change."""
        if mid_binance is None or self.mid_binance_at_last_change is None:
            return None
        return mid_binance - self.mid_binance_at_last_change

    def clear(self) -> None:
        """Reset on market rotation."""
        self._last_yes_bid = None
        self._last_yes_ask = None
        self._last_no_bid = None
        self._last_no_ask = None
        self.last_change_ts_ns = 0
        self.mid_binance_at_last_change = None


# ---------------------------------------------------------------------------
# Low liquidity detection
# ---------------------------------------------------------------------------


def is_low_liquidity(
    spread_yes: Optional[float], spread_no: Optional[float]
) -> bool:
    """True if book is effectively empty (spread >= 0.90)."""
    if spread_yes is not None and spread_yes >= _LOW_LIQUIDITY_SPREAD:
        return True
    if spread_no is not None and spread_no >= _LOW_LIQUIDITY_SPREAD:
        return True
    return False


# ---------------------------------------------------------------------------
# Tick builder — assembles all indicators into the output dict
# ---------------------------------------------------------------------------


def build_normalized_tick(
    state: SharedState,
    price_history: PriceHistory,
    poly_tracker: PolyChangeTracker,
) -> dict:
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

    # Fallback: set strike from Binance mid if still 0.0
    if mkt.strike == 0.0 and mid_binance is not None:
        mkt.strike = round(mid_binance, 2)
        log.info("Strike set from signal worker fallback: %.2f", mkt.strike)
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

    # --- Phase 1.1: Short returns ---
    if mid_binance is not None:
        price_history.push(now_ns, mid_binance)

    mid_1s = price_history.get_price_ago(now_ns, 1.0)
    mid_3s = price_history.get_price_ago(now_ns, 3.0)
    mid_5s = price_history.get_price_ago(now_ns, 5.0)

    ret_1s = compute_return(mid_binance, mid_1s)
    ret_3s = compute_return(mid_binance, mid_3s)
    ret_5s = compute_return(mid_binance, mid_5s)
    price_velocity = compute_velocity(ret_3s)

    # --- Phase 1.1: Poly change tracking ---
    poly_tracker.check_and_update(
        pm.yes_best_bid, pm.yes_best_ask,
        pm.no_best_bid, pm.no_best_ask,
        mid_binance, now_ns,
    )
    bn_move_since_poly = poly_tracker.compute_binance_move(mid_binance)

    # --- Phase 1.1: Low liquidity flag ---
    low_liq = is_low_liquidity(spread_yes, spread_no)

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
        "end_ts_ms": int(mkt.end_ts_s * 1000) if mkt.end_ts_s else None,
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
        # Divergence
        "delta_price": _r(delta_price, 6),
        # Phase 1.1: momentum
        "ret_1s": _r(ret_1s, 2),
        "ret_3s": _r(ret_3s, 2),
        "ret_5s": _r(ret_5s, 2),
        "price_velocity": _r(price_velocity, 4),
        # Phase 1.1: lag edge
        "bn_move_since_poly": _r(bn_move_since_poly, 2),
        # Phase 1.1: liquidity flag
        "low_liquidity": low_liq,
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

    # Phase 1.1 state
    price_history = PriceHistory()
    poly_tracker = PolyChangeTracker()

    # Wait for readiness — log what's missing every 5 seconds
    wait_count = 0
    while not state.shutdown.is_set() and not state.ready:
        await asyncio.sleep(0.5)
        wait_count += 1
        if wait_count % 10 == 0:  # every 5 seconds
            bn_ok = state.binance_ready
            pm_ok = state.polymarket_ready
            log.info(
                "Waiting for readiness: binance=%s (bid=%s) polymarket=%s (yes_bid=%s)",
                bn_ok, state.binance.best_bid,
                pm_ok, state.polymarket.yes_best_bid,
            )

    if state.shutdown.is_set():
        return

    log.info("Signal worker ready — both sources have data")

    last_condition_id = ""

    try:
        while not state.shutdown.is_set():
            t0 = asyncio.get_event_loop().time()

            # Clear history on market rotation
            cid = state.polymarket.market.condition_id
            if cid != last_condition_id:
                price_history.clear()
                poly_tracker.clear()
                last_condition_id = cid
                log.info("Signal worker: market rotated, history cleared")

            tick = build_normalized_tick(state, price_history, poly_tracker)

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
