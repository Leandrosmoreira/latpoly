"""Strategy 3 — Tick Scalp with Binance Signal (1-tick repeat).

Same Binance lag signal as S1/S2 (zscore + bn_move) for direction,
but with 1-tick profit target and rapid re-entry at the same price level.

Flow:
  1. Binance lag signal → decide YES or NO
  2. BUY maker at best_bid (6 shares)
  3. SELL maker at entry + 1 tick ($0.01)
  4. If sold → accumulate PnL, re-buy at same level
  5. Repeat until cycle target hit (10% of lot notional = $0.30)
  6. Market rotation → reset cycle, start fresh

Key difference from S2:
  - 1 tick profit (vs 2 ticks)
  - Preferred-price re-entry after successful sell
  - Shorter cooldown (3 ticks vs config default)
  - ~6-12 trades per cycle vs ~3-6

State machine:
  WORKING  — accepting signals, placing entries/exits
  DONE     — target hit, all activity blocked until next market_id

Config (env vars):
  LATPOLY_S3_ORDER_SIZE        = 6       (shares per order, buys 6 so 5 settle on-chain)
  LATPOLY_S3_PROFIT_TICKS      = 1       (exit = entry + 1 tick = $0.01)
  LATPOLY_S3_ZSCORE_THRESHOLD  = 1.5     (min zscore for signal)
  LATPOLY_S3_BN_MOVE_MIN       = 2.0     (min BTC move in USD)
  LATPOLY_S3_MAX_SPREAD        = 0.06    (max spread to enter)
  LATPOLY_S3_MIN_DEPTH         = 50      (min depth on ask)
  LATPOLY_S3_CYCLE_BASE_PRICE  = 0.50    (notional reference price)
  LATPOLY_S3_CYCLE_TARGET_PCT  = 0.10    (10% of lot notional = $0.30)
  LATPOLY_S3_MAX_POSITION      = 12      (max shares held at once)
  LATPOLY_S3_MIN_TTX           = 120     (min time-to-expiry in seconds)
  LATPOLY_S3_COOLDOWN_TICKS    = 3       (ticks between trades — short for rapid re-entry)
  LATPOLY_S3_MAX_DATA_AGE_MS   = 5000    (max data age in ms)
  LATPOLY_S3_RET1S_MIN         = 0.0     (min 1s momentum — 0 = disabled)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from latpoly.strategy.base import BaseStrategy
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import Signal

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))

def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


# ---------------------------------------------------------------------------
# Strategy 3 Engine
# ---------------------------------------------------------------------------


class Strategy3Engine(BaseStrategy):
    """Tick Scalp — 1-tick profit, Binance signal, rapid re-entry."""

    def __init__(self, cfg: StrategyConfig) -> None:
        super().__init__(cfg)

        # --- Strategy-specific config (from env vars) ---
        self._order_size = _env_int("LATPOLY_S3_ORDER_SIZE", 6)
        self._profit_ticks = _env_int("LATPOLY_S3_PROFIT_TICKS", 1)
        self._zscore_threshold = _env_float("LATPOLY_S3_ZSCORE_THRESHOLD", 1.5)
        self._bn_move_min = _env_float("LATPOLY_S3_BN_MOVE_MIN", 2.0)
        self._max_spread = _env_float("LATPOLY_S3_MAX_SPREAD", 0.06)
        self._min_depth = _env_float("LATPOLY_S3_MIN_DEPTH", 50.0)
        self._cycle_base_price = _env_float("LATPOLY_S3_CYCLE_BASE_PRICE", 0.50)
        self._cycle_target_pct = _env_float("LATPOLY_S3_CYCLE_TARGET_PCT", 0.10)
        self._max_position = _env_int("LATPOLY_S3_MAX_POSITION", 12)
        self._min_ttx = _env_float("LATPOLY_S3_MIN_TTX", 120.0)
        self._cooldown_ticks = _env_int("LATPOLY_S3_COOLDOWN_TICKS", 3)
        self._max_data_age_ms = _env_float("LATPOLY_S3_MAX_DATA_AGE_MS", 5000.0)
        self._ret1s_min = _env_float("LATPOLY_S3_RET1S_MIN", 0.0)

        # Derived: 10% of 1 lot notional (6 * $0.50 * 10% = $0.30)
        self._cycle_base_notional = self._order_size * self._cycle_base_price
        self._cycle_target_pnl = self._cycle_base_notional * self._cycle_target_pct

        # --- Cycle state (preserved across trades within same market) ---
        self._status = "WORKING"  # WORKING | DONE
        self._realized_pnl_cycle: float = 0.0
        self._trade_count_cycle: int = 0

        # --- Preferred re-entry price (key S3 feature) ---
        self._preferred_entry_price: Optional[float] = None
        self._preferred_side: str = ""  # last successful side

        # --- Per-tick state ---
        self._last_condition_id: str = ""
        self._last_entry_tick_idx: int = -9999
        self._current_position_shares: int = 0

        # --- Daily counters ---
        self._daily_trade_count: int = 0
        self._daily_pnl: float = 0.0

        log.info(
            "[strategy_3] init: order_size=%d profit_ticks=%d "
            "zscore_thresh=%.2f bn_move_min=%.1f cooldown=%d "
            "target=$%.2f (%.0f%%)",
            self._order_size, self._profit_ticks,
            self._zscore_threshold, self._bn_move_min,
            self._cooldown_ticks,
            self._cycle_target_pnl, self._cycle_target_pct * 100,
        )

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def on_tick(self, tick: dict, tick_idx: int) -> Signal:
        cid = tick.get("condition_id", "")

        # Market rotation → reset cycle
        if cid and cid != self._last_condition_id:
            if self._last_condition_id:
                self._reset_cycle(cid)
            self._last_condition_id = cid

        # DONE → block everything
        if self._status == "DONE":
            return Signal(
                action="NONE", side="", reason="cycle_target_hit",
            )

        # Kill switches
        if self._daily_trade_count >= self.cfg.max_daily_trades:
            return Signal(action="NONE", side="", reason="max_daily_trades")
        if self._daily_pnl <= -self.cfg.max_daily_loss:
            return Signal(action="NONE", side="", reason="max_daily_loss")

        # Time to expiry gate
        tte_ms = tick.get("time_to_expiry_ms")
        if tte_ms is None:
            return Signal(action="NONE", side="", reason="no_tte")
        tte_s = tte_ms / 1000.0
        if tte_s < self._min_ttx:
            return Signal(action="NONE", side="", reason="too_close_to_expiry")

        # Entry window (reuse from main config)
        if tte_s > self.cfg.entry_window_max_s:
            return Signal(action="NONE", side="", reason="too_early")

        # Position limit
        if self._current_position_shares >= self._max_position:
            return Signal(action="NONE", side="", reason="max_position")

        # --- Entry signal ---
        return self._check_entry(tick, tick_idx)

    def reset_daily(self) -> None:
        self._daily_trade_count = 0
        self._daily_pnl = 0.0

    def reset_trade(self) -> None:
        """Called by LiveTrader after each trade completes.

        Preserves cycle state + preferred entry price for re-entry.
        Only clears per-trade tracking.
        """
        self._current_position_shares = 0
        self._last_entry_tick_idx = -9999
        # NOTE: _preferred_entry_price and _preferred_side are preserved
        # so next on_tick() can re-enter at the same level

    def notify_trade_result(self, pnl: float) -> None:
        """Accumulate realized PnL for the cycle and check target."""
        self._realized_pnl_cycle += pnl
        self._trade_count_cycle += 1
        self._daily_trade_count += 1
        self._daily_pnl += pnl
        self._current_position_shares = 0

        log.info(
            "$$$ [strategy_3] trade #%d pnl=$%+.4f  "
            "cycle_realized=$%+.4f / $%.4f (%.1f%%)",
            self._trade_count_cycle, pnl,
            self._realized_pnl_cycle, self._cycle_target_pnl,
            (self._realized_pnl_cycle / self._cycle_target_pnl * 100)
            if self._cycle_target_pnl else 0,
        )

        # Check cycle target
        if self._realized_pnl_cycle >= self._cycle_target_pnl:
            self._status = "DONE"
            log.info(
                "$$$ [strategy_3] TARGET HIT! cycle_pnl=$%+.4f >= $%.4f  "
                "(%d trades). Blocking new entries until next market.",
                self._realized_pnl_cycle, self._cycle_target_pnl,
                self._trade_count_cycle,
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _reset_cycle(self, new_cid: str) -> None:
        """Full cycle reset on market rotation."""
        old_status = self._status
        old_pnl = self._realized_pnl_cycle
        old_trades = self._trade_count_cycle

        self._status = "WORKING"
        self._realized_pnl_cycle = 0.0
        self._trade_count_cycle = 0
        self._current_position_shares = 0
        self._last_entry_tick_idx = -9999
        self._preferred_entry_price = None
        self._preferred_side = ""

        log.info(
            "$$$ [strategy_3] CYCLE RESET: %s -> WORKING  "
            "(prev: %s, pnl=$%+.4f, %d trades)  new_market=%s",
            old_status, old_status, old_pnl, old_trades, new_cid[:12],
        )

    def _check_entry(self, tick: dict, tick_idx: int) -> Signal:
        """Check entry conditions — Binance lag signal + rapid re-entry."""

        # 1. Low liquidity
        if tick.get("low_liquidity", False):
            return Signal(action="NONE", side="", reason="low_liquidity")

        # 2. Data freshness
        age_bn = tick.get("age_binance_ms")
        age_pm = tick.get("age_poly_ms")
        if age_bn is None or age_pm is None:
            return Signal(action="NONE", side="", reason="no_age_data")
        if age_bn > self._max_data_age_ms or age_pm > self._max_data_age_ms:
            return Signal(action="NONE", side="", reason="stale_data")

        # 3. Distance to strike
        dist = tick.get("distance_to_strike")
        if dist is None or abs(dist) < self.cfg.min_distance_to_strike:
            return Signal(action="NONE", side="", reason="too_close_to_strike")

        # 4. Cooldown (shorter than S2 for rapid re-entry)
        if (tick_idx - self._last_entry_tick_idx) < self._cooldown_ticks:
            return Signal(action="NONE", side="", reason="cooldown")

        # 5. Determine side from Binance lag direction
        bn_move = tick.get("bn_move_since_poly")
        zscore = tick.get("zscore_bn_move")
        if bn_move is None or zscore is None:
            return Signal(action="NONE", side="", reason="no_lag_data")

        if bn_move > 0 and zscore >= self._zscore_threshold:
            side = "YES"
        elif bn_move < 0 and zscore <= -self._zscore_threshold:
            side = "NO"
        else:
            return Signal(action="NONE", side="", reason="zscore_below_threshold")

        # 6. Minimum BTC move
        bn_move_abs = abs(bn_move)
        if bn_move_abs < self._bn_move_min:
            return Signal(action="NONE", side="", reason="bn_move_too_small")

        # 7. Momentum confirmation (optional — disabled by default with ret1s_min=0)
        if self._ret1s_min > 0:
            ret_1s = tick.get("ret_1s")
            if ret_1s is None:
                return Signal(action="NONE", side="", reason="no_ret1s")
            if side == "YES" and ret_1s < self._ret1s_min:
                return Signal(action="NONE", side="", reason="momentum_not_confirmed")
            if side == "NO" and ret_1s > -self._ret1s_min:
                return Signal(action="NONE", side="", reason="momentum_not_confirmed")

        # 8. Probability range filter
        mid_key = "mid_yes" if side == "YES" else "mid_no"
        mid = tick.get(mid_key)
        if mid is not None:
            if mid < self.cfg.min_mid_entry or mid > self.cfg.max_mid_entry:
                return Signal(action="NONE", side="", reason=f"mid_out_of_range={mid:.4f}")

        # 9. Spread check (use strategy-specific max_spread)
        spread_key = "spread_yes" if side == "YES" else "spread_no"
        spread = tick.get(spread_key)
        if spread is None or spread > self._max_spread:
            return Signal(action="NONE", side="", reason="spread_too_wide")

        # 10. Depth check (use strategy-specific min_depth)
        depth_key = f"{'yes' if side == 'YES' else 'no'}_depth_ask_total"
        depth = tick.get(depth_key)
        if depth is not None and depth < self._min_depth:
            return Signal(action="NONE", side="", reason="insufficient_depth")

        # 11. Entry price — prefer previous successful price if same side
        prefix = "yes" if side == "YES" else "no"
        bid_key = f"pm_{prefix}_best_bid"
        market_bid = tick.get(bid_key)
        if market_bid is None:
            return Signal(action="NONE", side="", reason="no_bid_price")

        # Re-entry logic: if we just sold at this level and signal still
        # points same direction, prefer the same entry price
        if (
            self._preferred_entry_price is not None
            and self._preferred_side == side
            and abs(market_bid - self._preferred_entry_price) <= 0.01
        ):
            entry_price = self._preferred_entry_price
            reentry = True
        else:
            entry_price = market_bid
            reentry = False

        # 12. Size
        size = self._order_size

        # 13. Exit target — 1 tick above entry
        exit_target = round(entry_price + self._profit_ticks * 0.01, 2)

        # --- ENTRY SIGNAL ---
        self._last_entry_tick_idx = tick_idx
        self._current_position_shares += size

        # Save preferred price for next re-entry
        self._preferred_entry_price = entry_price
        self._preferred_side = side

        action = "BUY_YES" if side == "YES" else "BUY_NO"

        log.info(
            "$$$ [strategy_3] ENTRY SIGNAL: %s @ $%.2f sz=%d  "
            "exit=$%.2f  cycle=$%+.4f/$%.4f  trade#%d%s",
            action, entry_price, size, exit_target,
            self._realized_pnl_cycle, self._cycle_target_pnl,
            self._trade_count_cycle + 1,
            "  [RE-ENTRY same level]" if reentry else "",
        )

        return Signal(
            action=action,
            side=side,
            reason=f"strategy_3 lag={bn_move:.1f} zs={zscore:.2f}"
                   + (" reentry" if reentry else ""),
            entry_price=entry_price,
            exit_target=exit_target,
            size=size,
            net_edge=0.0,
            time_weight=1.0,
            tick_idx=tick_idx,
        )
