"""Strategy Engine — pure logic for latency arbitrage decisions.

Consumes normalized ticks (dicts) and emits Signal objects.
No I/O, no async — used identically by backtester, paper, and live.

Strategy:
- Entry: taker (aggressive) when Binance moved but Polymarket hasn't
- Exit: maker (limit order at target) or hold-to-expiry for near-certain outcomes
- Window: only trade between entry_window_max_s and entry_window_min_s before expiry
- Risk: momentum confirmation, minimum distance to strike, data freshness, cooldown
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from latpoly.strategy.config import StrategyConfig


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Signal:
    """A trading signal emitted by the engine."""

    action: str  # "BUY_YES", "BUY_NO", "EXIT", "HOLD_TO_EXPIRY", "NONE"
    side: str  # "YES" or "NO" or ""
    reason: str
    entry_price: Optional[float] = None
    exit_target: Optional[float] = None
    exit_price: Optional[float] = None  # for EXIT signals: price to close at
    size: int = 0
    net_edge: float = 0.0
    time_weight: float = 1.0
    tick_idx: int = 0


@dataclass
class Position:
    """An open position tracked by the engine."""

    side: str  # "YES" or "NO"
    entry_price: float
    size: int
    entry_tick_idx: int
    condition_id: str
    exit_target: Optional[float]
    hold_ticks: int = 0


# ---------------------------------------------------------------------------
# No-op signal (singleton-like)
# ---------------------------------------------------------------------------

_NONE_SIGNAL = Signal(action="NONE", side="", reason="no action")


# ---------------------------------------------------------------------------
# Strategy Engine
# ---------------------------------------------------------------------------


class StrategyEngine:
    """Stateful strategy engine. Feed ticks via on_tick(), get Signals back."""

    def __init__(self, cfg: StrategyConfig) -> None:
        self.cfg = cfg

        # Current state
        self._position: Optional[Position] = None
        self._last_condition_id: str = ""
        self._last_entry_tick_idx: int = -9999
        self._daily_trade_count: int = 0
        self._daily_pnl: float = 0.0
        self._last_tick: Optional[dict] = None  # for settlement

        # Trades log (for backtester to consume)
        self.closed_trades: list[dict] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_tick(self, tick: dict, tick_idx: int) -> Signal:
        """Process one tick. Returns a Signal (may be NONE)."""
        cid = tick.get("condition_id", "")

        # Market rotation: settle open position from previous market
        if cid and cid != self._last_condition_id:
            settle_signal = self._handle_rotation(tick, tick_idx)
            self._last_condition_id = cid
            if settle_signal.action != "NONE":
                return settle_signal

        self._last_tick = tick

        # Kill switches
        if self._daily_trade_count >= self.cfg.max_daily_trades:
            return Signal(action="NONE", side="", reason="max_daily_trades")
        if self._daily_pnl <= -self.cfg.max_daily_loss:
            return Signal(action="NONE", side="", reason="max_daily_loss")

        # Has open position? -> check exit
        if self._position is not None:
            return self._check_exit(tick, tick_idx)

        # No position -> check entry
        return self._check_entry(tick, tick_idx)

    def reset_daily(self) -> None:
        """Reset daily counters (call at UTC midnight)."""
        self._daily_trade_count = 0
        self._daily_pnl = 0.0

    # ------------------------------------------------------------------
    # Entry logic
    # ------------------------------------------------------------------

    def _check_entry(self, tick: dict, tick_idx: int) -> Signal:
        """Check if we should enter a new position."""
        cfg = self.cfg

        # 1. Time-to-expiry window
        tte_ms = tick.get("time_to_expiry_ms")
        if tte_ms is None:
            return Signal(action="NONE", side="", reason="no_tte")
        tte_s = tte_ms / 1000.0
        if tte_s > cfg.entry_window_max_s:
            return Signal(action="NONE", side="", reason="too_early")
        if tte_s < cfg.entry_window_min_s:
            return Signal(action="NONE", side="", reason="too_late")

        # 2. Low liquidity
        if tick.get("low_liquidity", False):
            return Signal(action="NONE", side="", reason="low_liquidity")

        # 3. Data freshness
        age_bn = tick.get("age_binance_ms")
        age_pm = tick.get("age_poly_ms")
        if age_bn is None or age_pm is None:
            return Signal(action="NONE", side="", reason="no_age_data")
        if age_bn > cfg.max_data_age_ms or age_pm > cfg.max_data_age_ms:
            return Signal(action="NONE", side="", reason="stale_data")

        # 4. Distance to strike
        dist = tick.get("distance_to_strike")
        if dist is None or abs(dist) < cfg.min_distance_to_strike:
            return Signal(action="NONE", side="", reason="too_close_to_strike")

        # 5. Cooldown
        if (tick_idx - self._last_entry_tick_idx) < cfg.cooldown_ticks:
            return Signal(action="NONE", side="", reason="cooldown")

        # 6. Determine side from lag direction
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

        # 7a. Minimum BTC move filter — small moves never cover the fee
        bn_move_abs = abs(bn_move)
        if bn_move_abs < cfg.min_bn_move_abs:
            return Signal(action="NONE", side="", reason="bn_move_too_small")

        # 7b. Momentum confirmation
        ret_1s = tick.get("ret_1s")
        if ret_1s is None:
            return Signal(action="NONE", side="", reason="no_ret1s")
        if side == "YES" and ret_1s < cfg.min_ret_1s_confirm:
            return Signal(action="NONE", side="", reason="momentum_not_confirmed")
        if side == "NO" and ret_1s > -cfg.min_ret_1s_confirm:
            return Signal(action="NONE", side="", reason="momentum_not_confirmed")

        # 8. Probability range filter — avoid extreme prices where PM doesn't react
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

        # 10. Depth check (Phase 2.1 data!)
        depth_key = f"{'yes' if side == 'YES' else 'no'}_depth_ask_total"
        depth = tick.get(depth_key)
        if depth is not None and depth < cfg.min_depth_contracts:
            return Signal(action="NONE", side="", reason="insufficient_depth")

        # 11. Compute entry price from VWAP (realistic slippage)
        prefix = "yes" if side == "YES" else "no"
        vwap_key = f"{prefix}_vwap_ask_100"
        entry_price = tick.get(vwap_key)
        if entry_price is None:
            # Fallback to best ask
            ask_key = f"pm_{prefix}_best_ask"
            entry_price = tick.get(ask_key)
        if entry_price is None:
            return Signal(action="NONE", side="", reason="no_ask_price")

        # 12. Compute slippage cost
        slip_key = f"{prefix}_slippage_ask_100"
        slippage = tick.get(slip_key) or 0.0

        # 13. Compute net edge
        edge_score = tick.get("edge_score")
        if edge_score is None:
            return Signal(action="NONE", side="", reason="no_edge_score")

        # Probability sensitivity: how much PM mid moves per $1 BTC
        # Near 0.50 -> max sensitivity (~0.001/$ BTC)
        # Near 0.10 or 0.90 -> low sensitivity (~0.0002/$ BTC)
        # Use parabolic: sensitivity = base * 4 * mid * (1 - mid)
        # At mid=0.50: 4*0.5*0.5 = 1.0x  At mid=0.20: 4*0.2*0.8 = 0.64x
        prob_sensitivity = 4.0 * mid * (1.0 - mid) if mid is not None else 0.5
        btc_to_pm_rate = cfg.btc_to_pm_base_rate * prob_sensitivity

        pm_edge_estimate = bn_move_abs * btc_to_pm_rate
        # Real Polymarket fee formula for entry
        entry_fee_per_contract = self.compute_taker_fee(
            entry_price, 1, cfg.taker_fee_rate, cfg.taker_fee_exponent
        )
        net_edge = pm_edge_estimate - slippage - entry_fee_per_contract

        # 14. Time weight
        time_weight = self._compute_time_weight(tte_s)

        # Adjusted min_net_edge by time weight (more lenient near expiry)
        if net_edge < cfg.min_net_edge / time_weight:
            return Signal(
                action="NONE", side="", reason="net_edge_too_low",
                net_edge=net_edge, time_weight=time_weight,
            )

        # 15. Compute dynamic exit target
        exit_target = self._compute_exit_target(entry_price, side, bn_move_abs, mid)

        # 16. Determine size
        size = min(cfg.base_size_contracts, cfg.max_position_contracts)

        # --- CREATE POSITION ---
        self._position = Position(
            side=side,
            entry_price=entry_price,
            size=size,
            entry_tick_idx=tick_idx,
            condition_id=tick.get("condition_id", ""),
            exit_target=exit_target,
        )
        self._last_entry_tick_idx = tick_idx

        action = "BUY_YES" if side == "YES" else "BUY_NO"
        return Signal(
            action=action,
            side=side,
            reason=f"lag={bn_move:.1f} zs={zscore:.2f} tw={time_weight:.2f}",
            entry_price=entry_price,
            exit_target=exit_target,
            size=size,
            net_edge=net_edge,
            time_weight=time_weight,
            tick_idx=tick_idx,
        )

    # ------------------------------------------------------------------
    # Exit logic
    # ------------------------------------------------------------------

    def _check_exit(self, tick: dict, tick_idx: int) -> Signal:
        """Check if we should exit the current position."""
        pos = self._position
        assert pos is not None
        pos.hold_ticks += 1

        tte_ms = tick.get("time_to_expiry_ms")
        tte_s = (tte_ms / 1000.0) if tte_ms is not None else 999.0

        # --- Hold to expiry ---
        dist = tick.get("distance_to_strike")
        if tte_s < 10.0 and dist is not None:
            if (pos.side == "YES" and dist > self.cfg.hold_to_expiry_distance) or \
               (pos.side == "NO" and dist < -self.cfg.hold_to_expiry_distance):
                return Signal(
                    action="HOLD_TO_EXPIRY", side=pos.side,
                    reason=f"dist={dist:.1f} tte={tte_s:.1f}s",
                    exit_price=1.0,  # will settle at $1.00
                    size=pos.size, tick_idx=tick_idx,
                )

        # --- Exit by target (maker) ---
        mid_key = "mid_yes" if pos.side == "YES" else "mid_no"
        mid = tick.get(mid_key)
        if mid is not None and pos.exit_target is not None:
            if mid >= pos.exit_target:
                exit_price = pos.exit_target  # maker fill at target
                pnl = self._compute_pnl(pos, exit_price, is_maker=True)
                self._record_trade(pos, exit_price, "MAKER", pnl, tick_idx)
                return Signal(
                    action="EXIT", side=pos.side,
                    reason=f"target_hit mid={mid:.4f} target={pos.exit_target:.4f}",
                    exit_price=exit_price, size=pos.size, tick_idx=tick_idx,
                )

        # --- Exit by reversal --- DISABLED: data shows reversals destroy profit
        # 2 reversal trades lost $11.57 vs 69 maker trades that gained $14.79
        # Keeping MAKER + TIMEOUT only until more data validates a better threshold
        # zscore = tick.get("zscore_bn_move")
        # if zscore is not None:
        #     if (pos.side == "YES" and zscore < -1.5) or \
        #        (pos.side == "NO" and zscore > 1.5):
        #         bid_key = f"{'yes' if pos.side == 'YES' else 'no'}_vwap_bid_100"
        #         exit_price = tick.get(bid_key)
        #         if exit_price is None:
        #             bid_key2 = f"pm_{'yes' if pos.side == 'YES' else 'no'}_best_bid"
        #             exit_price = tick.get(bid_key2)
        #         if exit_price is not None:
        #             pnl = self._compute_pnl(pos, exit_price, is_maker=False)
        #             self._record_trade(pos, exit_price, "REVERSAL", pnl, tick_idx)
        #             return Signal(
        #                 action="EXIT", side=pos.side,
        #                 reason=f"reversal zs={zscore:.2f}",
        #                 exit_price=exit_price, size=pos.size, tick_idx=tick_idx,
        #             )

        # --- Exit by stop-loss ---
        # If current price moved against us by more than stop_loss_per_contract, cut
        bid_key_sl = f"{'yes' if pos.side == 'YES' else 'no'}_vwap_bid_100"
        current_bid = tick.get(bid_key_sl)
        if current_bid is None:
            current_bid = tick.get(f"pm_{'yes' if pos.side == 'YES' else 'no'}_best_bid")
        if current_bid is not None:
            unrealized_loss = pos.entry_price - current_bid  # positive = losing
            if unrealized_loss >= self.cfg.stop_loss_per_contract:
                pnl = self._compute_pnl(pos, current_bid, is_maker=False)
                self._record_trade(pos, current_bid, "STOP_LOSS", pnl, tick_idx)
                return Signal(
                    action="EXIT", side=pos.side,
                    reason=f"stop_loss loss={unrealized_loss:.4f}/contract",
                    exit_price=current_bid, size=pos.size, tick_idx=tick_idx,
                )

        # --- Exit by timeout ---
        if pos.hold_ticks >= self.cfg.max_hold_ticks:
            bid_key = f"{'yes' if pos.side == 'YES' else 'no'}_vwap_bid_100"
            exit_price = tick.get(bid_key)
            if exit_price is None:
                bid_key2 = f"pm_{'yes' if pos.side == 'YES' else 'no'}_best_bid"
                exit_price = tick.get(bid_key2)
            if exit_price is None:
                exit_price = pos.entry_price  # worst case: flat
            pnl = self._compute_pnl(pos, exit_price, is_maker=False)
            self._record_trade(pos, exit_price, "TIMEOUT", pnl, tick_idx)
            return Signal(
                action="EXIT", side=pos.side,
                reason=f"timeout hold={pos.hold_ticks}",
                exit_price=exit_price, size=pos.size, tick_idx=tick_idx,
            )

        return Signal(action="NONE", side=pos.side, reason="holding")

    # ------------------------------------------------------------------
    # Market rotation / settlement
    # ------------------------------------------------------------------

    def _handle_rotation(self, tick: dict, tick_idx: int) -> Signal:
        """Settle any open position when market rotates."""
        if self._position is None:
            return _NONE_SIGNAL

        pos = self._position
        last = self._last_tick or tick

        # Determine settlement from last tick's distance_to_strike
        dist = last.get("distance_to_strike", 0)
        if dist is None:
            dist = 0

        if dist > 0:
            yes_price, no_price = 1.0, 0.0
        elif dist < 0:
            yes_price, no_price = 0.0, 1.0
        else:
            yes_price, no_price = 0.5, 0.5

        exit_price = yes_price if pos.side == "YES" else no_price

        # Determine if it was a win
        pnl = self._compute_pnl(pos, exit_price, is_maker=False, is_settlement=True)
        exit_type = "EXPIRY_WIN" if pnl > 0 else "EXPIRY_LOSS"
        self._record_trade(pos, exit_price, exit_type, pnl, tick_idx)

        return Signal(
            action="EXIT", side=pos.side,
            reason=f"settlement dist={dist:.1f} -> {exit_type}",
            exit_price=exit_price, size=pos.size, tick_idx=tick_idx,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compute_time_weight(self, tte_s: float) -> float:
        """Linear interpolation: tw_min at max_s -> tw_max at min_s."""
        cfg = self.cfg
        max_s = cfg.entry_window_max_s
        min_s = cfg.entry_window_min_s
        if max_s <= min_s:
            return cfg.time_weight_max
        # Clamp tte_s to window
        t = max(min_s, min(max_s, tte_s))
        # Linear: at max_s -> tw_min, at min_s -> tw_max
        frac = (max_s - t) / (max_s - min_s)
        return cfg.time_weight_min + frac * (cfg.time_weight_max - cfg.time_weight_min)

    def _compute_exit_target(
        self, entry_price: float, side: str, bn_move_abs: float,
        mid: float | None = None,
    ) -> float:
        """Dynamic exit target based on BTC lag magnitude and probability sensitivity.

        Near mid=0.50 probability is most sensitive to BTC moves.
        Near extremes (0.10, 0.90) targets are smaller.
        """
        prob_sensitivity = 4.0 * mid * (1.0 - mid) if mid is not None else 0.5
        btc_to_pm_rate = self.cfg.btc_to_pm_base_rate * prob_sensitivity
        pm_move = bn_move_abs * btc_to_pm_rate
        target_profit = pm_move * self.cfg.exit_profit_fraction
        target_profit = max(0.005, min(target_profit, 0.05))  # clamp
        return entry_price + target_profit

    @staticmethod
    def compute_taker_fee(price: float, size: int, fee_rate: float, fee_exp: float) -> float:
        """Polymarket taker fee: size * price * fee_rate * (price * (1 - price))^exponent.

        Max effective rate = 1.56% at price=0.50 (with rate=0.25, exp=2).
        Fee → 0 at extremes (price near 0 or 1).
        """
        if price <= 0.0 or price >= 1.0:
            return 0.0
        fee = size * price * fee_rate * (price * (1.0 - price)) ** fee_exp
        return round(fee, 4)  # Polymarket rounds to 4 decimal places

    def _compute_pnl(
        self,
        pos: Position,
        exit_price: float,
        is_maker: bool,
        is_settlement: bool = False,
    ) -> float:
        """Compute net P&L for closing a position.

        Entry is always taker (pays real Polymarket fee formula).
        Exit is maker (0% fee) for target fills, taker for timeout/stop.
        Settlement has no fees.

        Polymarket rule: maker orders require min_maker_size shares.
        If position < min_maker_size, forced to exit as taker.
        """
        cfg = self.cfg

        # Entry cost: price * size + taker fee
        entry_fee = self.compute_taker_fee(
            pos.entry_price, pos.size, cfg.taker_fee_rate, cfg.taker_fee_exponent
        )
        entry_cost = pos.entry_price * pos.size + entry_fee

        if is_settlement:
            exit_revenue = exit_price * pos.size
        elif is_maker and pos.size >= cfg.min_maker_size:
            # Maker exit: 0% fee
            exit_revenue = exit_price * pos.size
        else:
            # Taker exit: real fee formula
            exit_fee = self.compute_taker_fee(
                exit_price, pos.size, cfg.taker_fee_rate, cfg.taker_fee_exponent
            )
            exit_revenue = exit_price * pos.size - exit_fee

        return exit_revenue - entry_cost

    def _record_trade(
        self, pos: Position, exit_price: float, exit_type: str, pnl: float, tick_idx: int
    ) -> None:
        """Record a completed trade and update counters."""
        self.closed_trades.append({
            "condition_id": pos.condition_id,
            "side": pos.side,
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "size": pos.size,
            "pnl_net": round(pnl, 4),
            "exit_type": exit_type,
            "hold_ticks": pos.hold_ticks,
            "entry_tick_idx": pos.entry_tick_idx,
            "exit_tick_idx": tick_idx,
        })

        self._daily_trade_count += 1
        self._daily_pnl += pnl
        self._position = None  # close position
