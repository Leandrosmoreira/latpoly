"""Strategy 4 — Lag Entry + Inventory Control + Time Stop.

Adapts the analyst's proposal to live_trader architecture (1 position per slot).
Uses Binance lag signal like S2, but adds:
  - Inventory control (Avellaneda simplified): zscore threshold escalates with trades
  - Side blocking: after 2 consecutive losses on same side, blocks that side
  - Time gates: 3 tiers (120s/90s/60s) for entry freezing
  - Max trades per cycle cap (8)

Flow:
  1. Binance lag signal → decide YES or NO
  2. Side blocked? → SKIP
  3. Zscore < dynamic threshold? → SKIP
  4. TTX < 120s? → SKIP (time freeze)
  5. BUY maker → wait fill → SELL maker (live_trader handles)
  6. notify_trade_result(pnl):
     - Win → reset loss counter for that side
     - Loss → increment; if ≥ 2 consecutive → BLOCK side
  7. cycle_pnl ≥ $0.30 → DONE
  8. Market rotation → full reset

State machine:
  WORKING  — accepting signals
  DONE     — target hit, blocked until next market_id

Config (env vars):
  LATPOLY_S4_ORDER_SIZE          = 6
  LATPOLY_S4_PROFIT_TICKS        = 2
  LATPOLY_S4_ZSCORE_BASE         = 1.5
  LATPOLY_S4_ZSCORE_CAUTIOUS     = 2.0
  LATPOLY_S4_ZSCORE_SELECTIVE    = 2.5
  LATPOLY_S4_ZSCORE_TIGHT        = 3.5
  LATPOLY_S4_ZSCORE_DEFENSIVE    = 999.0
  LATPOLY_S4_BN_MOVE_MIN         = 6.0
  LATPOLY_S4_MAX_SAME_SIDE_LOSSES = 2
  LATPOLY_S4_MAX_CYCLE_TRADES    = 8
  LATPOLY_S4_FREEZE_TTX          = 120
  LATPOLY_S4_FORCE_TTX           = 90
  LATPOLY_S4_EXIT_TTX            = 60
  LATPOLY_S4_CYCLE_BASE_PRICE    = 0.50
  LATPOLY_S4_CYCLE_TARGET_PCT    = 0.10
  LATPOLY_S4_MAX_DATA_AGE_MS     = 5000
  LATPOLY_S4_COOLDOWN_TICKS      = 5
  LATPOLY_S4_MAX_SPREAD          = 0.08
  LATPOLY_S4_MIN_DEPTH           = 50
  LATPOLY_S4_RET1S_MIN           = 0.0
"""

from __future__ import annotations

import logging
import os

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
# Strategy 4 Engine
# ---------------------------------------------------------------------------


class Strategy4Engine(BaseStrategy):
    """Lag Entry + Inventory Control + Time Stop."""

    def __init__(self, cfg: StrategyConfig) -> None:
        super().__init__(cfg)

        # --- Strategy-specific config (from env vars) ---
        self._order_size = _env_int("LATPOLY_S4_ORDER_SIZE", 6)
        self._profit_ticks = _env_int("LATPOLY_S4_PROFIT_TICKS", 2)
        self._zscore_base = _env_float("LATPOLY_S4_ZSCORE_BASE", 1.5)
        self._zscore_cautious = _env_float("LATPOLY_S4_ZSCORE_CAUTIOUS", 2.0)
        self._zscore_selective = _env_float("LATPOLY_S4_ZSCORE_SELECTIVE", 2.5)
        self._zscore_tight = _env_float("LATPOLY_S4_ZSCORE_TIGHT", 3.5)
        self._zscore_defensive = _env_float("LATPOLY_S4_ZSCORE_DEFENSIVE", 999.0)
        self._bn_move_min = _env_float("LATPOLY_S4_BN_MOVE_MIN", 6.0)
        self._max_same_side_losses = _env_int("LATPOLY_S4_MAX_SAME_SIDE_LOSSES", 2)
        self._max_cycle_trades = _env_int("LATPOLY_S4_MAX_CYCLE_TRADES", 8)
        self._freeze_ttx = _env_float("LATPOLY_S4_FREEZE_TTX", 120.0)
        self._force_ttx = _env_float("LATPOLY_S4_FORCE_TTX", 90.0)
        self._exit_ttx = _env_float("LATPOLY_S4_EXIT_TTX", 60.0)
        self._cycle_base_price = _env_float("LATPOLY_S4_CYCLE_BASE_PRICE", 0.50)
        self._cycle_target_pct = _env_float("LATPOLY_S4_CYCLE_TARGET_PCT", 0.10)
        self._max_data_age_ms = _env_float("LATPOLY_S4_MAX_DATA_AGE_MS", 5000.0)
        self._cooldown_ticks = _env_int("LATPOLY_S4_COOLDOWN_TICKS", 5)
        self._max_spread = _env_float("LATPOLY_S4_MAX_SPREAD", 0.08)
        self._min_depth = _env_float("LATPOLY_S4_MIN_DEPTH", 50.0)
        self._ret1s_min = _env_float("LATPOLY_S4_RET1S_MIN", 0.0)

        # Derived
        self._cycle_base_notional = self._order_size * self._cycle_base_price
        self._cycle_target_pnl = self._cycle_base_notional * self._cycle_target_pct

        # --- Cycle state ---
        self._status = "WORKING"  # WORKING | DONE
        self._realized_pnl_cycle: float = 0.0
        self._trade_count_cycle: int = 0

        # --- Inventory / side blocking ---
        self._yes_consecutive_losses: int = 0
        self._no_consecutive_losses: int = 0
        self._yes_blocked: bool = False
        self._no_blocked: bool = False
        self._last_trade_side: str = ""  # "YES" or "NO"

        # --- Per-tick state ---
        self._last_condition_id: str = ""
        self._last_entry_tick_idx: int = -9999

        # --- Daily counters ---
        self._daily_trade_count: int = 0
        self._daily_pnl: float = 0.0

        log.info(
            "[strategy_4] init: order_size=%d profit_ticks=%d "
            "zscore=%.1f/%.1f/%.1f/%.1f/%.0f "
            "bn_move_min=%.1f max_side_losses=%d max_cycle_trades=%d "
            "freeze_ttx=%.0fs target=$%.2f (%.0f%%)",
            self._order_size, self._profit_ticks,
            self._zscore_base, self._zscore_cautious,
            self._zscore_selective, self._zscore_tight,
            self._zscore_defensive,
            self._bn_move_min,
            self._max_same_side_losses, self._max_cycle_trades,
            self._freeze_ttx,
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
            return Signal(action="NONE", side="", reason="cycle_target_hit")

        # Kill switches
        if self._daily_trade_count >= self.cfg.max_daily_trades:
            return Signal(action="NONE", side="", reason="max_daily_trades")
        if self._daily_pnl <= -self.cfg.max_daily_loss:
            return Signal(action="NONE", side="", reason="max_daily_loss")

        # Max cycle trades cap
        if self._trade_count_cycle >= self._max_cycle_trades:
            return Signal(
                action="NONE", side="",
                reason=f"max_cycle_trades={self._max_cycle_trades}",
            )

        # Both sides blocked → effectively done for this cycle
        if self._yes_blocked and self._no_blocked:
            return Signal(
                action="NONE", side="",
                reason="both_sides_blocked",
            )

        # Time to expiry gate — 3 tiers
        tte_ms = tick.get("time_to_expiry_ms")
        if tte_ms is None:
            return Signal(action="NONE", side="", reason="no_tte")
        tte_s = tte_ms / 1000.0

        if tte_s < self._exit_ttx:
            return Signal(action="NONE", side="", reason="time_exit_zone")
        if tte_s < self._force_ttx:
            return Signal(action="NONE", side="", reason="time_force_zone")
        if tte_s < self._freeze_ttx:
            return Signal(action="NONE", side="", reason="time_freeze_zone")

        # Entry window (reuse from main config)
        if tte_s > self.cfg.entry_window_max_s:
            return Signal(action="NONE", side="", reason="too_early")

        # --- Entry signal ---
        return self._check_entry(tick, tick_idx)

    def reset_daily(self) -> None:
        self._daily_trade_count = 0
        self._daily_pnl = 0.0

    def reset_trade(self) -> None:
        """Called by LiveTrader after each trade completes.

        Preserves cycle state, inventory tracking, side blocks.
        Only clears per-trade tracking.
        """
        self._last_entry_tick_idx = -9999

    def notify_trade_result(self, pnl: float) -> None:
        """Accumulate PnL and update inventory/side tracking."""
        self._realized_pnl_cycle += pnl
        self._trade_count_cycle += 1
        self._daily_trade_count += 1
        self._daily_pnl += pnl

        side = self._last_trade_side

        # --- Side loss tracking ---
        # pnl > 0 = win (reset counter), pnl < 0 = loss (increment),
        # pnl == 0 = breakeven (neutral — don't touch counters)
        if pnl > 0:
            # Win → reset consecutive loss counter for this side
            if side == "YES":
                self._yes_consecutive_losses = 0
            elif side == "NO":
                self._no_consecutive_losses = 0
            log.info(
                "$$$ [strategy_4] WIN trade #%d side=%s pnl=$%+.4f  "
                "loss counters: YES=%d NO=%d",
                self._trade_count_cycle, side, pnl,
                self._yes_consecutive_losses, self._no_consecutive_losses,
            )
        elif pnl < 0:
            # Loss → increment counter, check for blocking
            if side == "YES":
                self._yes_consecutive_losses += 1
                if self._yes_consecutive_losses >= self._max_same_side_losses:
                    self._yes_blocked = True
                    log.warning(
                        "$$$ [strategy_4] YES side BLOCKED after %d "
                        "consecutive losses",
                        self._yes_consecutive_losses,
                    )
            elif side == "NO":
                self._no_consecutive_losses += 1
                if self._no_consecutive_losses >= self._max_same_side_losses:
                    self._no_blocked = True
                    log.warning(
                        "$$$ [strategy_4] NO side BLOCKED after %d "
                        "consecutive losses",
                        self._no_consecutive_losses,
                    )
            log.info(
                "$$$ [strategy_4] LOSS trade #%d side=%s pnl=$%+.4f  "
                "loss counters: YES=%d%s NO=%d%s",
                self._trade_count_cycle, side, pnl,
                self._yes_consecutive_losses,
                " [BLOCKED]" if self._yes_blocked else "",
                self._no_consecutive_losses,
                " [BLOCKED]" if self._no_blocked else "",
            )
        else:
            # Breakeven (pnl == 0) — neutral, don't change loss counters
            log.info(
                "$$$ [strategy_4] BREAKEVEN trade #%d side=%s pnl=$0.0000  "
                "loss counters unchanged: YES=%d NO=%d",
                self._trade_count_cycle, side,
                self._yes_consecutive_losses, self._no_consecutive_losses,
            )

        # Cycle PnL log
        log.info(
            "$$$ [strategy_4] cycle: trade #%d  pnl=$%+.4f  "
            "cycle_realized=$%+.4f / $%.4f (%.1f%%)  "
            "zscore_thresh=%.1f",
            self._trade_count_cycle, pnl,
            self._realized_pnl_cycle, self._cycle_target_pnl,
            (self._realized_pnl_cycle / self._cycle_target_pnl * 100)
            if self._cycle_target_pnl else 0,
            self._get_zscore_threshold(),
        )

        # Check cycle target
        if self._realized_pnl_cycle >= self._cycle_target_pnl:
            self._status = "DONE"
            log.info(
                "$$$ [strategy_4] TARGET HIT! cycle_pnl=$%+.4f >= $%.4f  "
                "(%d trades). Blocking new entries until next market.",
                self._realized_pnl_cycle, self._cycle_target_pnl,
                self._trade_count_cycle,
            )

    # ------------------------------------------------------------------
    # Inventory control — dynamic zscore threshold
    # ------------------------------------------------------------------

    def _get_zscore_threshold(self) -> float:
        """Return dynamic zscore threshold based on inventory exposure.

        Avellaneda-inspired: more trades in cycle → higher bar to enter.
        Gradual escalation so all 8 trade slots are usable:
          trades 0-1: base     (1.5) — normal
          trades 2-3: cautious (2.0) — slightly stricter
          trades 4-5: selective(2.5) — only strong signals
          trades 6-7: tight    (3.5) — very strong signals only
          trades 8+:  defensive(999) — blocked (at cap)
        """
        # Both sides blocked → defensive (effectively stop)
        if self._yes_blocked and self._no_blocked:
            return self._zscore_defensive

        trades = self._trade_count_cycle
        if trades <= 1:
            return self._zscore_base       # 1.5 — normal
        elif trades <= 3:
            return self._zscore_cautious   # 2.0 — cautious
        elif trades <= 5:
            return self._zscore_selective  # 2.5 — selective
        elif trades <= 7:
            return self._zscore_tight      # 3.5 — tight
        else:
            return self._zscore_defensive  # 999 — blocked (at cap)

    # ------------------------------------------------------------------
    # Internal — entry logic
    # ------------------------------------------------------------------

    def _check_entry(self, tick: dict, tick_idx: int) -> Signal:
        """Check entry conditions — Binance lag + inventory awareness."""

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

        # 4. Cooldown
        if (tick_idx - self._last_entry_tick_idx) < self._cooldown_ticks:
            return Signal(action="NONE", side="", reason="cooldown")

        # 5. Determine side from Binance lag direction
        bn_move = tick.get("bn_move_since_poly")
        zscore = tick.get("zscore_bn_move")
        if bn_move is None or zscore is None:
            return Signal(action="NONE", side="", reason="no_lag_data")

        # Get dynamic threshold
        zs_threshold = self._get_zscore_threshold()

        if bn_move > 0 and zscore >= zs_threshold:
            side = "YES"
        elif bn_move < 0 and zscore <= -zs_threshold:
            side = "NO"
        else:
            return Signal(
                action="NONE", side="",
                reason=f"zscore_below_threshold={zscore:.2f}<{zs_threshold:.1f}",
            )

        # 6. Side blocking check
        if side == "YES" and self._yes_blocked:
            return Signal(action="NONE", side="", reason="yes_side_blocked")
        if side == "NO" and self._no_blocked:
            return Signal(action="NONE", side="", reason="no_side_blocked")

        # 7. Minimum BTC move
        bn_move_abs = abs(bn_move)
        if bn_move_abs < self._bn_move_min:
            return Signal(action="NONE", side="", reason="bn_move_too_small")

        # 8. Momentum confirmation (optional — disabled by default with ret1s_min=0)
        if self._ret1s_min > 0:
            ret_1s = tick.get("ret_1s")
            if ret_1s is None:
                return Signal(action="NONE", side="", reason="no_ret1s")
            if side == "YES" and ret_1s < self._ret1s_min:
                return Signal(action="NONE", side="", reason="momentum_not_confirmed")
            if side == "NO" and ret_1s > -self._ret1s_min:
                return Signal(action="NONE", side="", reason="momentum_not_confirmed")

        # 9. Probability range filter
        mid_key = "mid_yes" if side == "YES" else "mid_no"
        mid = tick.get(mid_key)
        if mid is not None:
            if mid < self.cfg.min_mid_entry or mid > self.cfg.max_mid_entry:
                return Signal(
                    action="NONE", side="",
                    reason=f"mid_out_of_range={mid:.4f}",
                )

        # 10. Spread check
        spread_key = "spread_yes" if side == "YES" else "spread_no"
        spread = tick.get(spread_key)
        if spread is None or spread > self._max_spread:
            return Signal(action="NONE", side="", reason="spread_too_wide")

        # 11. Depth check
        depth_key = f"{'yes' if side == 'YES' else 'no'}_depth_ask_total"
        depth = tick.get(depth_key)
        if depth is not None and depth < self._min_depth:
            return Signal(action="NONE", side="", reason="insufficient_depth")

        # 12. Entry price (maker: best_bid)
        prefix = "yes" if side == "YES" else "no"
        bid_key = f"pm_{prefix}_best_bid"
        entry_price = tick.get(bid_key)
        if entry_price is None:
            return Signal(action="NONE", side="", reason="no_bid_price")

        # 13. Size
        size = self._order_size

        # 14. Exit target
        exit_target = round(entry_price + self._profit_ticks * 0.01, 2)

        # --- ENTRY SIGNAL ---
        self._last_entry_tick_idx = tick_idx
        self._last_trade_side = side

        action = "BUY_YES" if side == "YES" else "BUY_NO"

        log.info(
            "$$$ [strategy_4] ENTRY SIGNAL: %s @ $%.2f sz=%d  "
            "exit=$%.2f  zscore=%.2f (thresh=%.1f)  "
            "cycle=$%+.4f/$%.4f  trade#%d  "
            "blocks: YES=%s NO=%s",
            action, entry_price, size, exit_target,
            zscore, zs_threshold,
            self._realized_pnl_cycle, self._cycle_target_pnl,
            self._trade_count_cycle + 1,
            "BLOCKED" if self._yes_blocked else "ok",
            "BLOCKED" if self._no_blocked else "ok",
        )

        return Signal(
            action=action,
            side=side,
            reason=f"strategy_4 lag={bn_move:.1f} zs={zscore:.2f}"
                   f" thresh={zs_threshold:.1f}",
            entry_price=entry_price,
            exit_target=exit_target,
            size=size,
            net_edge=0.0,
            time_weight=1.0,
            tick_idx=tick_idx,
        )

    # ------------------------------------------------------------------
    # Cycle management
    # ------------------------------------------------------------------

    def _reset_cycle(self, new_cid: str) -> None:
        """Full cycle reset on market rotation — clears everything."""
        old_status = self._status
        old_pnl = self._realized_pnl_cycle
        old_trades = self._trade_count_cycle

        self._status = "WORKING"
        self._realized_pnl_cycle = 0.0
        self._trade_count_cycle = 0
        self._last_entry_tick_idx = -9999

        # Reset inventory / side blocking
        self._yes_consecutive_losses = 0
        self._no_consecutive_losses = 0
        self._yes_blocked = False
        self._no_blocked = False
        self._last_trade_side = ""

        log.info(
            "$$$ [strategy_4] CYCLE RESET: %s -> WORKING  "
            "(prev: %s, pnl=$%+.4f, %d trades)  new_market=%s",
            old_status, old_status, old_pnl, old_trades, new_cid[:12],
        )
