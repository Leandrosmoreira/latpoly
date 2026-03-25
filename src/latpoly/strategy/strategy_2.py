"""Strategy 2 — Cycle TP 10% Maker-Only.

Uses the same Binance lag signal as the scalp strategy, but allows multiple
sequential trades per market cycle.  Stops completely when cumulative realized
PnL for the cycle hits +10% of base notional.  Resets on market rotation.

State machine:
  WORKING  — accepting signals, placing entries/exits
  DONE     — target hit, all activity blocked until next market_id

Config (env vars):
  LATPOLY_S2_ORDER_SIZE       = 5       (shares per order)
  LATPOLY_S2_PROFIT_TICKS     = 2       (exit = entry + N ticks)
  LATPOLY_S2_CYCLE_BASE_PRICE = 0.50    (notional reference price)
  LATPOLY_S2_CYCLE_TARGET_PCT = 0.10    (10% of base notional)
  LATPOLY_S2_MAX_POSITION     = 15      (max shares held at once)
  LATPOLY_S2_MIN_TTX          = 120     (no new entries within Ns of expiry)
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
# Strategy 2 Engine
# ---------------------------------------------------------------------------


class CycleTP10Engine(BaseStrategy):
    """Cycle TP 10% — multiple maker trades per market, stop at +10%."""

    def __init__(self, cfg: StrategyConfig) -> None:
        super().__init__(cfg)

        # --- Strategy-specific config (from env vars) ---
        self._order_size = _env_int("LATPOLY_S2_ORDER_SIZE", 5)
        self._profit_ticks = _env_int("LATPOLY_S2_PROFIT_TICKS", 2)
        self._cycle_base_price = _env_float("LATPOLY_S2_CYCLE_BASE_PRICE", 0.50)
        self._cycle_target_pct = _env_float("LATPOLY_S2_CYCLE_TARGET_PCT", 0.10)
        self._max_position = _env_int("LATPOLY_S2_MAX_POSITION", 15)
        self._min_ttx = _env_float("LATPOLY_S2_MIN_TTX", 120.0)

        # Derived
        self._cycle_base_notional = self._order_size * self._cycle_base_price
        self._cycle_target_pnl = self._cycle_base_notional * self._cycle_target_pct

        # --- Cycle state ---
        self._status = "WORKING"  # WORKING | DONE
        self._realized_pnl_cycle: float = 0.0
        self._trade_count_cycle: int = 0

        # --- Per-tick state ---
        self._last_condition_id: str = ""
        self._last_entry_tick_idx: int = -9999
        self._current_position_shares: int = 0  # tracked by LiveTrader, but engine uses for gating

        # --- Daily counters ---
        self._daily_trade_count: int = 0
        self._daily_pnl: float = 0.0

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

        # Kill switches (reuse from main config)
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

        # Position limit — engine doesn't track exact position (LiveTrader does),
        # but _current_position_shares is updated via notify/reset
        if self._current_position_shares >= self._max_position:
            return Signal(action="NONE", side="", reason="max_position")

        # --- Entry signal (same Binance lag logic as scalp) ---
        return self._check_entry(tick, tick_idx)

    def reset_daily(self) -> None:
        self._daily_trade_count = 0
        self._daily_pnl = 0.0

    def reset_trade(self) -> None:
        """Called by LiveTrader after each trade completes.

        Preserves cycle state (realized_pnl, status, trade_count).
        Only clears per-trade tracking.
        """
        self._current_position_shares = 0
        self._last_entry_tick_idx = -9999

    def notify_trade_result(self, pnl: float) -> None:
        """Accumulate realized PnL for the cycle and check target."""
        self._realized_pnl_cycle += pnl
        self._trade_count_cycle += 1
        self._daily_trade_count += 1
        self._daily_pnl += pnl
        self._current_position_shares = 0

        log.info(
            "$$$ [cycle_tp10] trade #%d pnl=$%+.4f  "
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
                "$$$ [cycle_tp10] TARGET HIT! cycle_pnl=$%+.4f >= $%.4f  "
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

        log.info(
            "$$$ [cycle_tp10] CYCLE RESET: %s -> WORKING  "
            "(prev: %s, pnl=$%+.4f, %d trades)  new_market=%s",
            old_status, old_status, old_pnl, old_trades, new_cid[:12],
        )

    def _check_entry(self, tick: dict, tick_idx: int) -> Signal:
        """Check entry conditions — same Binance lag signal as scalp, simplified."""
        cfg = self.cfg

        # 1. Low liquidity
        if tick.get("low_liquidity", False):
            return Signal(action="NONE", side="", reason="low_liquidity")

        # 2. Data freshness
        age_bn = tick.get("age_binance_ms")
        age_pm = tick.get("age_poly_ms")
        if age_bn is None or age_pm is None:
            return Signal(action="NONE", side="", reason="no_age_data")
        if age_bn > cfg.max_data_age_ms or age_pm > cfg.max_data_age_ms:
            return Signal(action="NONE", side="", reason="stale_data")

        # 3. Distance to strike
        dist = tick.get("distance_to_strike")
        if dist is None or abs(dist) < cfg.min_distance_to_strike:
            return Signal(action="NONE", side="", reason="too_close_to_strike")

        # 4. Cooldown
        if (tick_idx - self._last_entry_tick_idx) < cfg.cooldown_ticks:
            return Signal(action="NONE", side="", reason="cooldown")

        # 5. Determine side from lag direction
        bn_move = tick.get("bn_move_since_poly")
        zscore = tick.get("zscore_bn_move")
        if bn_move is None or zscore is None:
            return Signal(action="NONE", side="", reason="no_lag_data")

        if bn_move > 0 and zscore >= cfg.zscore_entry_threshold:
            side = "YES"
        elif bn_move < 0 and zscore <= -cfg.zscore_entry_threshold:
            side = "NO"
        else:
            return Signal(action="NONE", side="", reason="zscore_below_threshold")

        # 6. Minimum BTC move
        bn_move_abs = abs(bn_move)
        if bn_move_abs < cfg.min_bn_move_abs:
            return Signal(action="NONE", side="", reason="bn_move_too_small")

        # 7. Momentum confirmation
        ret_1s = tick.get("ret_1s")
        if ret_1s is None:
            return Signal(action="NONE", side="", reason="no_ret1s")
        if side == "YES" and ret_1s < cfg.min_ret_1s_confirm:
            return Signal(action="NONE", side="", reason="momentum_not_confirmed")
        if side == "NO" and ret_1s > -cfg.min_ret_1s_confirm:
            return Signal(action="NONE", side="", reason="momentum_not_confirmed")

        # 8. Probability range filter
        mid_key = "mid_yes" if side == "YES" else "mid_no"
        mid = tick.get(mid_key)
        if mid is not None:
            if mid < cfg.min_mid_entry or mid > cfg.max_mid_entry:
                return Signal(action="NONE", side="", reason=f"mid_out_of_range={mid:.4f}")

        # 9. Spread check
        spread_key = "spread_yes" if side == "YES" else "spread_no"
        spread = tick.get(spread_key)
        if spread is None or spread > cfg.max_spread_entry:
            return Signal(action="NONE", side="", reason="spread_too_wide")

        # 10. Depth check
        depth_key = f"{'yes' if side == 'YES' else 'no'}_depth_ask_total"
        depth = tick.get(depth_key)
        if depth is not None and depth < cfg.min_depth_contracts:
            return Signal(action="NONE", side="", reason="insufficient_depth")

        # 11. Entry price (maker: best_bid)
        prefix = "yes" if side == "YES" else "no"
        bid_key = f"pm_{prefix}_best_bid"
        entry_price = tick.get(bid_key)
        if entry_price is None:
            return Signal(action="NONE", side="", reason="no_bid_price")

        # 12. Size = fixed order size
        size = self._order_size

        # 13. Exit target
        exit_ticks = self._profit_ticks
        exit_target = round(entry_price + exit_ticks * 0.01, 2)

        # --- ENTRY SIGNAL ---
        self._last_entry_tick_idx = tick_idx
        self._current_position_shares += size

        action = "BUY_YES" if side == "YES" else "BUY_NO"

        log.info(
            "$$$ [cycle_tp10] ENTRY SIGNAL: %s @ $%.2f sz=%d  "
            "exit_target=$%.2f  cycle_pnl=$%+.4f/%+.4f  trade#%d",
            action, entry_price, size, exit_target,
            self._realized_pnl_cycle, self._cycle_target_pnl,
            self._trade_count_cycle + 1,
        )

        return Signal(
            action=action,
            side=side,
            reason=f"cycle_tp10 lag={bn_move:.1f} zs={zscore:.2f}",
            entry_price=entry_price,
            exit_target=exit_target,
            size=size,
            net_edge=0.0,
            time_weight=1.0,
            tick_idx=tick_idx,
        )
