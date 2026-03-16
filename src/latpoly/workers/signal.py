"""W3 — Signal fusion worker: polls SharedState and produces normalized ticks.

Each indicator is computed by a small, pure function that returns
Optional[float] — None means "data not available yet", which is
semantically different from 0.0 and must be preserved in the output.

Phase 1.1 additions:
- ret_1s, ret_3s, ret_5s: short Binance returns
- price_velocity: smoothed USD/s speed (ret_3s / 3.0)
- binance_move_since_last_poly_update: lag edge metric
- low_liquidity: flag for empty/artificial books

Phase 2 additions:
- zscore_bn_move: EMA-based z-score for lag edge outlier detection
- zscore_ret1s: EMA-based z-score for momentum outlier detection
- poly_reaction_ms: time since last Polymarket book change
- time_bucket: market regime category by time-to-expiry
- edge_score: composite signal combining lag, momentum, and liquidity
"""

from __future__ import annotations

import asyncio
import logging
import math
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

# EMA smoothing factor for z-score computation
_EMA_ALPHA = 0.05

# Minimum ticks before z-score is considered valid (warmup)
_ZSCORE_WARMUP = 20


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
# Phase 2: EMA-based Z-Score tracker (O(1), no arrays)
# ---------------------------------------------------------------------------


class EmaZScore:
    """Exponentially weighted z-score tracker.

    Uses EMA for mean and variance — O(1) per update, no buffers.
    Returns None during warmup period (_ZSCORE_WARMUP ticks).
    """

    __slots__ = ("_mean", "_var", "_count", "_alpha")

    def __init__(self, alpha: float = _EMA_ALPHA) -> None:
        self._mean: float = 0.0
        self._var: float = 0.0
        self._count: int = 0
        self._alpha = alpha

    def update(self, x: float) -> Optional[float]:
        """Feed a new value, return z-score (or None during warmup)."""
        self._count += 1

        if self._count == 1:
            # First observation: initialize mean, variance unknown
            self._mean = x
            self._var = 0.0
            return None

        # EMA update
        a = self._alpha
        delta = x - self._mean
        self._mean = a * x + (1.0 - a) * self._mean
        self._var = a * (delta * delta) + (1.0 - a) * self._var

        if self._count < _ZSCORE_WARMUP:
            return None  # not enough data yet

        std = math.sqrt(self._var) if self._var > 0 else 0.0
        if std < 1e-10:
            return 0.0  # no variance = no outlier
        return delta / std

    def clear(self) -> None:
        """Reset on market rotation."""
        self._mean = 0.0
        self._var = 0.0
        self._count = 0


# ---------------------------------------------------------------------------
# Phase 2: Time bucket categorization
# ---------------------------------------------------------------------------


def compute_time_bucket(time_to_expiry_ms: Optional[float]) -> Optional[str]:
    """Categorize market regime by time-to-expiry.

    Near expiry = more urgency = larger lag edges matter more.
    """
    if time_to_expiry_ms is None:
        return None
    s = time_to_expiry_ms / 1000.0
    if s > 600:
        return ">600s"
    if s > 300:
        return "600-300s"
    if s > 120:
        return "300-120s"
    if s > 60:
        return "120-60s"
    return "<60s"


# ---------------------------------------------------------------------------
# Phase 2: Composite edge score
# ---------------------------------------------------------------------------


def compute_edge_score(
    bn_move_since_poly: Optional[float],
    zscore_bn_move: Optional[float],
    ret_1s: Optional[float],
    low_liquidity: bool,
    time_to_expiry_ms: Optional[float],
) -> Optional[float]:
    """Composite score combining lag magnitude, z-score, and momentum.

    Higher absolute value = stronger lag edge signal.
    Returns None if insufficient data. Sign indicates direction.

    Formula:
        edge = bn_move * (1 + |zscore| * 0.5) * urgency_multiplier
        - discounted to 0 if low_liquidity (can't trade it)
        - urgency increases near expiry (last 120s)
    """
    if bn_move_since_poly is None:
        return None
    if low_liquidity:
        return 0.0  # can't trade, no edge

    # Base: raw lag
    edge = bn_move_since_poly

    # Amplify by z-score (outlier = more confident signal)
    if zscore_bn_move is not None:
        edge *= (1.0 + abs(zscore_bn_move) * 0.5)

    # Momentum confirmation: same direction = stronger
    if ret_1s is not None:
        if (bn_move_since_poly > 0 and ret_1s > 0) or \
           (bn_move_since_poly < 0 and ret_1s < 0):
            edge *= 1.2  # momentum confirms lag direction

    # Urgency: near expiry, the edge is more actionable
    if time_to_expiry_ms is not None:
        s = time_to_expiry_ms / 1000.0
        if s < 60:
            edge *= 1.5  # last minute
        elif s < 120:
            edge *= 1.2  # last 2 minutes

    return edge


# ---------------------------------------------------------------------------
# Tick builder — assembles all indicators into the output dict
# ---------------------------------------------------------------------------


def build_normalized_tick(
    state: SharedState,
    price_history: PriceHistory,
    poly_tracker: PolyChangeTracker,
    zscore_bn_move: EmaZScore,
    zscore_ret1s: EmaZScore,
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

    # --- Phase 2: Z-scores (EMA-based, O(1)) ---
    zs_bn = zscore_bn_move.update(bn_move_since_poly) if bn_move_since_poly is not None else None
    zs_r1 = zscore_ret1s.update(ret_1s) if ret_1s is not None else None

    # --- Phase 2: Poly reaction time ---
    poly_react_ms: Optional[float] = None
    if poly_tracker.last_change_ts_ns > 0:
        poly_react_ms = (now_ns - poly_tracker.last_change_ts_ns) / 1e6

    # --- Phase 2: Time bucket ---
    time_bucket = compute_time_bucket(time_to_expiry_ms)

    # --- Phase 2: Composite edge score ---
    edge_score = compute_edge_score(
        bn_move_since_poly, zs_bn, ret_1s, low_liq, time_to_expiry_ms,
    )

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
        # Phase 2: z-scores
        "zscore_bn_move": _r(zs_bn, 4),
        "zscore_ret1s": _r(zs_r1, 4),
        # Phase 2: poly reaction & regime
        "poly_reaction_ms": _r(poly_react_ms, 2),
        "time_bucket": time_bucket,
        # Phase 2: composite edge
        "edge_score": _r(edge_score, 4),
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

    # Phase 2 state
    zscore_bn_move = EmaZScore()
    zscore_ret1s = EmaZScore()

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
                zscore_bn_move.clear()
                zscore_ret1s.clear()
                last_condition_id = cid
                log.info("Signal worker: market rotated, history cleared")

            tick = build_normalized_tick(
                state, price_history, poly_tracker,
                zscore_bn_move, zscore_ret1s,
            )

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
