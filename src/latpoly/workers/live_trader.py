"""W5-live -- Live trading worker with scalp exit (+N ticks).

Mirrors paper_trader_worker but places real orders via Polymarket CLOB API.
Only operates on BTC slots (filters out ETH due to low liquidity).

Order lifecycle:
1. Engine BUY signal  -> place GTC limit BUY at best_bid (maker, 0% fee)
   - BUY size: min 5 shares (maker minimum). With residual, rounds up to next
     multiple of 5 (e.g. residual=2 -> buy 8 -> total 10 = 2 SELL lots)
2. Freeze slot for MIN_ORDER_LIFETIME_S (~7s, don't feed engine, let order fill)
3. After freeze, cancel order to check fill status:
   - "canceled"        -> entry never filled (~60-70%), engine resets, done
   - "matched"/"gone"  -> entry filled (~30-40%), place SELL
   - partial fill      -> if total >= 5 -> place SELL; else accumulate residual
4. SELL order placed at max(entry, ceil(mid + 2*$0.01)) (maker, 0% fee)
5. SELL repaint loop: every 7s, cancel-to-check, recalculate price, re-place
   - SELL filled -> profit taken, position cleared, engine resets for next trade
   - SELL partial -> save residual (<5 = wait for next BUY; >=5 = repaint SELL)
   - SELL not filled + >7s before expiry -> repaint SELL at current mid + 2 ticks
   - SELL not filled + <7s before expiry -> cancel SELL, hold to auto-settlement
6. Market rotation -> cancel all orders, filled positions auto-settle at $0/$1
7. Shutdown -> cancel all orders, filled positions auto-settle

Logs trades to data/live/ in same JSONL format as paper_trader.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Optional

from latpoly.config import Config
from latpoly.execution.poly_client import PolyClient
from latpoly.shared_state import SharedState
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import Signal, StrategyEngine

log = logging.getLogger(__name__)

# Polymarket tick size for crypto up/down markets
TICK_SIZE = 0.01

# Minimum seconds an entry order must live before we check fill status.
# Without this, the engine exits on the very next tick (~0.2s) and the
# maker order never has time to fill on the exchange.
MIN_ORDER_LIFETIME_S = float(os.environ.get("LATPOLY_MIN_ORDER_LIFE", "7"))

# Seconds before expiry to cancel SELL and hold to auto-settlement
EXPIRY_CANCEL_S = float(os.environ.get("LATPOLY_EXPIRY_CANCEL_S", "7"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ceil_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price UP to nearest tick (for sell prices)."""
    return math.ceil(price / tick) * tick


def _floor_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price DOWN to nearest tick (for buy prices)."""
    return math.floor(price / tick) * tick


# ---------------------------------------------------------------------------
# Tracked orders
# ---------------------------------------------------------------------------


@dataclass
class TrackedEntry:
    """A real entry order placed on the exchange."""

    order_id: str
    slot_id: str
    token_id: str
    side: str  # YES / NO
    price: float
    size: int
    tick_idx: int
    created_at: float = field(default_factory=time.time)


@dataclass
class TrackedExit:
    """A real SELL order placed after entry fill."""

    order_id: str
    slot_id: str
    token_id: str
    side: str  # YES / NO
    entry_price: float
    sell_price: float
    size: int
    created_at: float = field(default_factory=time.time)
    check_count: int = 0  # how many times we've checked this order


# ---------------------------------------------------------------------------
# Live Trader
# ---------------------------------------------------------------------------


class LiveTrader:
    """Live trading wrapper -- places real orders based on engine signals.

    Uses the same StrategyEngine as paper_trader. The engine tracks virtual
    positions; this class shadows them with real Polymarket orders.

    Flow after entry fill:
    1. Place SELL maker at entry + fixed_exit_ticks * $0.01
    2. Wait MIN_ORDER_LIFETIME_S, then cancel-to-check
    3. If filled -> scalp profit taken, reset engine, ready for next trade
    4. If not filled + time left -> re-place SELL, repeat
    5. If not filled + <EXPIRY_CANCEL_S before expiry -> hold to expiry
    """

    def __init__(
        self,
        strat_cfg: StrategyConfig,
        slot_ids: list[str],
        poly: PolyClient,
        output_dir: str = "data/live",
    ) -> None:
        self.strat_cfg = strat_cfg
        self._engines: dict[str, StrategyEngine] = {
            sid: StrategyEngine(strat_cfg) for sid in slot_ids
        }
        self._tick_idx: dict[str, int] = {sid: 0 for sid in slot_ids}
        self._poly = poly

        # Output files
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._trade_file: Optional[IO] = None
        self._signal_file: Optional[IO] = None
        self._current_date: str = ""

        # Real order tracking: slot_id -> pending entry order
        self._entry_orders: dict[str, TrackedEntry] = {}

        # Filled positions: slot_id -> entry that was confirmed filled
        self._filled_positions: dict[str, TrackedEntry] = {}

        # Exit orders: slot_id -> pending SELL order
        self._exit_orders: dict[str, TrackedExit] = {}

        # Background tasks for async order placement
        self._pending_tasks: list[asyncio.Task] = []

        # Session stats
        self._session_trades: int = 0
        self._session_pnl: float = 0.0
        self._session_wins: int = 0
        self._orders_placed: int = 0
        self._orders_filled: int = 0
        self._orders_cancelled: int = 0
        self._sells_placed: int = 0
        self._sells_filled: int = 0
        self._sells_expired: int = 0  # held to expiry instead of SELL fill

        # Per-slot stats
        self._slot_trades: dict[str, int] = {sid: 0 for sid in slot_ids}
        self._slot_pnl: dict[str, float] = {sid: 0.0 for sid in slot_ids}

        # Residual shares: slot_id -> (side, token_id, shares, avg_entry_price)
        # When a SELL partially fills, leftover shares are tracked here.
        # Next BUY adjusts size to complete a full lot (multiple of min_maker_size).
        self._residual: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Lot management
    # ------------------------------------------------------------------

    def _get_residual(self, slot_id: str) -> int:
        """Get residual shares for a slot (0 if none)."""
        r = self._residual.get(slot_id)
        return r["shares"] if r else 0

    def _compute_buy_size(self, slot_id: str) -> int:
        """Compute next BUY size, accounting for residual shares.

        Constraints:
        - BUY order itself must be >= min_maker_size (5) for maker pricing
        - Total (residual + buy) should be a multiple of min_maker_size for clean SELL lots

        Examples (min_maker_size=5, base_size=5):
          residual=0 -> buy 5   (total=5,  1 SELL lot)
          residual=1 -> buy 9   (total=10, 2 SELL lots)
          residual=2 -> buy 8   (total=10, 2 SELL lots)
          residual=3 -> buy 7   (total=10, 2 SELL lots)
          residual=4 -> buy 6   (total=10, 2 SELL lots)
          residual=5 -> buy 5   (total=10, 2 SELL lots)
          residual=7 -> buy 8   (total=15, 3 SELL lots)
        """
        min_lot = self.strat_cfg.min_maker_size  # 5
        base = self.strat_cfg.base_size_contracts  # 5
        residual = self._get_residual(slot_id)

        if residual <= 0:
            return base  # no residual, buy normal lot

        # Need total to be next multiple of min_lot above (residual + min_lot)
        # so that both BUY >= min_lot AND total is a clean multiple
        target_total = ((residual + min_lot) // min_lot) * min_lot
        # If that target doesn't give us a buy >= min_lot, go up one more multiple
        if target_total - residual < min_lot:
            target_total += min_lot
        return target_total - residual

    def _compute_sell_size(self, slot_id: str, filled_size: int) -> int:
        """Compute SELL size: residual + newly filled shares.

        Must be >= min_maker_size for maker order.
        """
        residual = self._get_residual(slot_id)
        total = residual + filled_size
        return total

    # ------------------------------------------------------------------
    # File rotation
    # ------------------------------------------------------------------

    def _rotate_files(self) -> None:
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today == self._current_date:
            return
        if self._trade_file is not None:
            self._trade_file.close()
        if self._signal_file is not None:
            self._signal_file.close()
        self._current_date = today
        try:
            self._trade_file = open(
                self._output_dir / f"live_trades_{today}.jsonl", "a"
            )
            self._signal_file = open(
                self._output_dir / f"live_signals_{today}.jsonl", "a"
            )
        except OSError:
            log.exception("Cannot open live output files")
        log.info("Live trader: writing to %s", self._output_dir / f"live_trades_{today}.jsonl")

    # ------------------------------------------------------------------
    # Token ID lookup
    # ------------------------------------------------------------------

    @staticmethod
    def _get_token_id(state: SharedState, slot_id: str, side: str) -> str:
        pm = state.get_polymarket(slot_id)
        if side == "YES":
            return pm.market.yes_token_id
        return pm.market.no_token_id

    # ------------------------------------------------------------------
    # Main tick handler
    # ------------------------------------------------------------------

    async def on_tick(self, tick: dict, state: SharedState) -> Signal:
        """Process one tick: manage orders, run engine, execute signals."""
        self._rotate_files()

        slot_id = tick.get("slot_id", "unknown")
        engine = self._engines.get(slot_id)
        if engine is None:
            return Signal(action="NONE", side="", reason="unknown_slot")

        self._tick_idx[slot_id] = self._tick_idx.get(slot_id, 0) + 1
        tick_idx = self._tick_idx[slot_id]

        # -- PHASE 0: Check pending SELL orders (scalp exit) ------------
        exit_order = self._exit_orders.get(slot_id)
        if exit_order is not None:
            result = await self._check_exit_order(slot_id, tick)
            if result == "filled":
                # Scalp profit taken! Clear and continue to next trade
                pass  # position already cleared in _check_exit_order
            elif result == "holding":
                # SELL still pending, don't feed engine
                return Signal(action="NONE", side="", reason="sell_pending")
            elif result == "expired":
                # Near expiry, holding to auto-settle
                pass
            # If "cancelled" or cleared, continue to engine

        # -- PHASE 1: Check pending entry orders ----------------------
        pending = self._entry_orders.get(slot_id)
        if pending is not None:
            age = time.time() - pending.created_at
            if age < MIN_ORDER_LIFETIME_S:
                # Order still young -- don't feed engine, let it fill
                return Signal(action="NONE", side="", reason="order_pending")

            # Order is old enough -- check fill status via cancel
            result = await self._poly.cancel_order(pending.order_id)
            self._entry_orders.pop(slot_id, None)

            if result in ("matched", "gone", "failed"):
                # Entry was FILLED (fully or partially)
                # Check actual filled size via API
                filled_size = await self._poly.get_filled_size(pending.order_id)
                if filled_size <= 0:
                    # API didn't return size -- assume full fill
                    filled_size = pending.size

                self._orders_filled += 1

                # Merge with any residual shares on this slot
                residual = self._residual.get(slot_id)
                if residual and residual["side"] == pending.side:
                    # Same side -- accumulate
                    old_shares = residual["shares"]
                    old_cost = residual["avg_entry"] * old_shares
                    new_cost = pending.price * filled_size
                    total_shares = old_shares + filled_size
                    avg_entry = (old_cost + new_cost) / total_shares
                    self._residual[slot_id] = {
                        "side": pending.side,
                        "token_id": pending.token_id,
                        "shares": total_shares,
                        "avg_entry": round(avg_entry, 4),
                    }
                    log.info(
                        ">>> [%s] Entry FILLED %d shares (residual %d + new %d = %d) "
                        "avg_entry=$%.4f",
                        slot_id, filled_size, old_shares, filled_size,
                        total_shares, avg_entry,
                    )
                else:
                    # No residual or different side -- fresh position
                    self._residual[slot_id] = {
                        "side": pending.side,
                        "token_id": pending.token_id,
                        "shares": filled_size,
                        "avg_entry": pending.price,
                    }
                    log.info(
                        ">>> [%s] Entry FILLED after %.1fs (cancel=%s) -- "
                        "%s @ $%.2f sz=%d (of %d ordered)",
                        slot_id, age, result,
                        pending.side, pending.price, filled_size, pending.size,
                    )

                # Update tracked entry with actual fill size
                pending.size = self._residual[slot_id]["shares"]
                pending.price = self._residual[slot_id]["avg_entry"]
                self._filled_positions[slot_id] = pending

                # Check if total shares >= min_maker_size for SELL
                total = self._residual[slot_id]["shares"]
                min_lot = self.strat_cfg.min_maker_size

                if total >= min_lot:
                    # Enough for maker SELL -- place exit order
                    await self._place_exit_order(slot_id, pending, state)
                    return Signal(action="NONE", side="", reason="entry_filled_sell_placed")
                else:
                    # Not enough for maker SELL -- wait for next BUY to accumulate
                    log.info(
                        ">>> [%s] Only %d shares (need %d for maker SELL) -- "
                        "waiting for next BUY to accumulate",
                        slot_id, total, min_lot,
                    )
                    # Reset engine to allow next entry
                    self._engines[slot_id] = StrategyEngine(self.strat_cfg)
                    # Don't clear filled_positions -- keep tracking
                    # But allow engine to generate new BUY signal
                    self._filled_positions.pop(slot_id, None)
                    return Signal(action="NONE", side="", reason="partial_fill_accumulating")
            else:
                # Entry NOT filled -- cancel succeeded
                self._orders_cancelled += 1
                log.info(
                    "--- [%s] Entry NOT filled after %.1fs -- cancelled. "
                    "Resetting engine.",
                    slot_id, age,
                )
                # Reset the engine so it doesn't think it has a position
                self._engines[slot_id] = StrategyEngine(self.strat_cfg)
                return Signal(action="NONE", side="", reason="entry_cancelled")

        # -- PHASE 2: If we have a filled position with no SELL, skip engine
        if slot_id in self._filled_positions and slot_id not in self._exit_orders:
            # This shouldn't happen normally, but just in case:
            # Re-place the SELL order
            filled = self._filled_positions[slot_id]
            log.warning(
                "!!! [%s] Filled position without SELL order -- re-placing",
                slot_id,
            )
            await self._place_exit_order(slot_id, filled, state)
            return Signal(action="NONE", side="", reason="sell_replaced")

        # -- PHASE 3: Run strategy engine (only if no position/orders) --
        signal = engine.on_tick(tick, tick_idx)

        if signal.action == "NONE":
            return signal

        now_iso = time.strftime("%H:%M:%S", time.gmtime())

        # -- PHASE 4: Handle ENTRY signals ----------------------------
        if signal.action in ("BUY_YES", "BUY_NO"):
            # Don't enter if we already have a filled position on this slot
            # (unless accumulating residual from partial fill)
            if slot_id in self._filled_positions:
                return Signal(action="NONE", side="", reason="already_filled")

            # Check if residual exists but wrong side -- clear it
            residual = self._residual.get(slot_id)
            if residual and residual["side"] != signal.side:
                log.warning(
                    "!!! [%s] Residual %d %s shares but signal is %s -- "
                    "residual will auto-settle at expiry",
                    slot_id, residual["shares"], residual["side"], signal.side,
                )
                self._residual.pop(slot_id, None)

            token_id = self._get_token_id(state, slot_id, signal.side)
            if not token_id:
                log.warning("[%s] No token_id for %s -- skipping entry", slot_id, signal.side)
                return signal

            # Compute buy size: accounts for residual shares
            buy_size = self._compute_buy_size(slot_id)
            residual_shares = self._get_residual(slot_id)

            # Place 1 tick above best_bid for queue priority
            pm = state.get_polymarket(slot_id)
            if signal.side == "YES":
                best_bid = pm.yes_best_bid or signal.entry_price
            else:
                best_bid = pm.no_best_bid or signal.entry_price
            entry_price = round(best_bid + TICK_SIZE, 2)

            total_after = residual_shares + buy_size
            log.info(
                ">>> [%s] LIVE ENTRY: %s %s buy=%d @ $%.2f (residual=%d, total=%d) "
                "-> sell=$%.2f  [%s]",
                slot_id, signal.action, signal.side, buy_size,
                entry_price, residual_shares, total_after,
                entry_price + self.strat_cfg.fixed_exit_ticks * TICK_SIZE,
                signal.reason,
            )

            self._write_signal({
                "ts": now_iso,
                "slot_id": slot_id,
                "action": signal.action,
                "side": signal.side,
                "entry_price": entry_price,
                "exit_target": entry_price + self.strat_cfg.fixed_exit_ticks * TICK_SIZE,
                "size": buy_size,
                "residual": residual_shares,
                "total_after": total_after,
                "reason": signal.reason,
                "token_id": token_id[:16],
                "tick_idx": tick_idx,
            })

            # Place real BUY order (adjusted size for lot management)
            await self._place_entry(
                slot_id, token_id, signal.side,
                entry_price, buy_size, tick_idx,
            )

        # -- PHASE 5: Handle EXIT/HOLD signals from engine (informational only)
        elif signal.action == "EXIT":
            # Engine says exit, but we manage exits via SELL orders
            # Just log it -- actual exit is handled by SELL order fill
            log.debug(
                "[%s] Engine EXIT signal (managed by SELL order): %s",
                slot_id, signal.reason,
            )

        elif signal.action == "HOLD_TO_EXPIRY":
            log.info(
                "=== [%s] LIVE HOLD_TO_EXPIRY: %s -- %s",
                slot_id, signal.side, signal.reason,
            )

        # Clean up completed tasks
        self._pending_tasks = [t for t in self._pending_tasks if not t.done()]

        return signal

    # ------------------------------------------------------------------
    # SELL order management (scalp exit)
    # ------------------------------------------------------------------

    async def _place_exit_order(
        self, slot_id: str, entry: TrackedEntry, state: SharedState,
    ) -> None:
        """Place SELL maker order at entry + fixed_exit_ticks * $0.01."""
        exit_ticks = self.strat_cfg.fixed_exit_ticks
        if exit_ticks <= 0:
            # No fixed exit -- hold to expiry (old behavior)
            log.info(
                "=== [%s] No fixed exit ticks -- holding to expiry",
                slot_id,
            )
            return

        sell_price = round(entry.price + exit_ticks * TICK_SIZE, 2)

        # Clamp sell_price to valid range (0.01 - 0.99)
        sell_price = max(0.01, min(0.99, sell_price))

        log.info(
            "<<< [%s] Placing SELL: %s @ $%.2f (entry=$%.2f + %d ticks) sz=%d",
            slot_id, entry.side, sell_price, entry.price, exit_ticks, entry.size,
        )

        oid = await self._poly.place_limit_sell(
            entry.token_id, sell_price, entry.size,
        )

        if oid:
            self._exit_orders[slot_id] = TrackedExit(
                order_id=oid,
                slot_id=slot_id,
                token_id=entry.token_id,
                side=entry.side,
                entry_price=entry.price,
                sell_price=sell_price,
                size=entry.size,
            )
            self._sells_placed += 1
            log.info(
                "<<< [%s] SELL order placed: %s @ $%.2f sz=%d  "
                "(will check fill in %.0fs)",
                slot_id, oid[:16], sell_price, entry.size, MIN_ORDER_LIFETIME_S,
            )
        else:
            log.warning(
                "!!! [%s] SELL order FAILED to place -- holding to expiry",
                slot_id,
            )

    def _compute_sell_price(self, exit_order: TrackedExit, tick: dict) -> float:
        """Compute SELL price: mid + 2 ticks, floored at entry price.

        Uses the current mid-price (avg of best_bid and best_ask) as
        reference, then adds 2 ticks ($0.02) for maker profit.

        Floor at entry + 2 ticks ensures we always sell for a profit.
        If mid drops too low, we keep SELL at entry+2 and wait for
        the market to recover or hold to expiry.

        Examples (entry=$0.49, exit_ticks=2):
          Market UP:   mid=$0.53 -> sell=$0.55 (profit $0.06/share)
          Market FLAT: mid=$0.50 -> sell=$0.52 (profit $0.03/share)
          Market DOWN: mid=$0.46 -> sell=$0.51 (floor=entry+2, min profit $0.02)
        """
        prefix = "yes" if exit_order.side == "YES" else "no"
        bid_key = f"pm_{prefix}_best_bid"
        ask_key = f"pm_{prefix}_best_ask"

        best_bid = tick.get(bid_key)
        best_ask = tick.get(ask_key)

        # Calculate mid price
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask
        else:
            # No market data -- fallback to entry + fixed ticks
            exit_ticks = self.strat_cfg.fixed_exit_ticks
            return round(exit_order.entry_price + exit_ticks * TICK_SIZE, 2)

        # Sell at mid + 2 ticks
        sell_price = _ceil_tick(mid + 2 * TICK_SIZE)

        # Floor: never sell below entry + exit_ticks (worst case = minimum profit)
        # This prevents repainting down to breakeven when mid drops
        exit_ticks = self.strat_cfg.fixed_exit_ticks
        floor_price = round(exit_order.entry_price + exit_ticks * TICK_SIZE, 2)

        # Clamp to valid range
        return max(floor_price, min(0.99, sell_price))

    async def _check_exit_order(self, slot_id: str, tick: dict) -> str:
        """Check if a pending SELL order has been filled.

        On each check cycle:
        1. If near expiry -> cancel SELL, hold to auto-settle
        2. If SELL was filled -> record profit, clear position, reset engine
        3. If SELL not filled -> REPAINT at best available price (dynamic target)

        Returns:
            "filled"    -- SELL was filled, profit taken
            "holding"   -- SELL still pending, keep waiting
            "expired"   -- Near expiry, cancelled SELL, holding to settle
            "cancelled" -- SELL cancelled for other reason
        """
        exit_order = self._exit_orders.get(slot_id)
        if exit_order is None:
            return "cancelled"

        # Check time to expiry -- cancel SELL if too close
        tte_ms = tick.get("time_to_expiry_ms")
        if tte_ms is not None:
            tte_s = tte_ms / 1000.0
            if tte_s < EXPIRY_CANCEL_S:
                # Too close to expiry -- cancel SELL and hold to auto-settle
                await self._poly.cancel_order(exit_order.order_id)
                self._exit_orders.pop(slot_id, None)
                self._sells_expired += 1
                log.info(
                    "=== [%s] SELL cancelled (%.1fs to expiry) -- holding to auto-settle. "
                    "%s entry=$%.2f sell=$%.2f",
                    slot_id, tte_s, exit_order.side,
                    exit_order.entry_price, exit_order.sell_price,
                )
                return "expired"

        # Check if SELL order is old enough to check
        age = time.time() - exit_order.created_at
        if age < MIN_ORDER_LIFETIME_S:
            return "holding"

        # Cancel-to-check fill status
        result = await self._poly.cancel_order(exit_order.order_id)
        exit_order.check_count += 1

        if result in ("matched", "gone", "failed"):
            # SELL was FILLED (fully or partially)
            # Check actual sold quantity
            sold_size = await self._poly.get_filled_size(exit_order.order_id)
            if sold_size <= 0:
                # API didn't return -- assume full fill
                sold_size = exit_order.size

            self._sells_filled += 1
            pnl = (exit_order.sell_price - exit_order.entry_price) * sold_size
            # Maker exit = 0% fee, so pnl is clean

            # Check for partial fill residual
            remaining = exit_order.size - sold_size
            if remaining > 0:
                # Partial SELL -- save residual for next cycle
                self._residual[slot_id] = {
                    "side": exit_order.side,
                    "token_id": exit_order.token_id,
                    "shares": remaining,
                    "avg_entry": exit_order.entry_price,
                }
                log.info(
                    ">>> [%s] SELL partial: sold %d of %d, residual=%d shares",
                    slot_id, sold_size, exit_order.size, remaining,
                )
            else:
                # Full fill -- clear residual
                self._residual.pop(slot_id, None)

            self._session_trades += 1
            self._session_pnl += pnl
            self._slot_trades[slot_id] = self._slot_trades.get(slot_id, 0) + 1
            self._slot_pnl[slot_id] = self._slot_pnl.get(slot_id, 0.0) + pnl
            if pnl > 0:
                self._session_wins += 1

            wr = (
                self._session_wins / self._session_trades * 100
                if self._session_trades
                else 0
            )

            now_iso = time.strftime("%H:%M:%S", time.gmtime())
            log.info(
                "+++ [%s] SELL FILLED! %s sold=%d entry=$%.2f sell=$%.2f pnl=$%+.4f  "
                "residual=%d  [session: %d trades, %.0f%% WR, $%+.2f]",
                slot_id, exit_order.side, sold_size,
                exit_order.entry_price, exit_order.sell_price, pnl,
                remaining,
                self._session_trades, wr, self._session_pnl,
            )

            self._write_trade({
                "ts": now_iso,
                "slot_id": slot_id,
                "side": exit_order.side,
                "entry_price": exit_order.entry_price,
                "exit_price": exit_order.sell_price,
                "size": sold_size,
                "size_ordered": exit_order.size,
                "residual": remaining,
                "pnl_net": round(pnl, 4),
                "exit_type": "SCALP_MAKER" if remaining == 0 else "SCALP_PARTIAL",
                "hold_ticks": exit_order.check_count,
                "session_pnl": round(self._session_pnl, 4),
                "session_trades": self._session_trades,
                "session_win_rate": round(wr, 1),
            })

            # Clear position and exit order
            self._exit_orders.pop(slot_id, None)
            self._filled_positions.pop(slot_id, None)

            # Reset engine -- ready for next trade on this slot
            self._engines[slot_id] = StrategyEngine(self.strat_cfg)

            return "filled"

        else:
            # SELL not filled -- REPAINT at best current price
            # Dynamic: MAX(entry + 2 ticks, current_bid + 1 tick)
            new_sell_price = self._compute_sell_price(exit_order, tick)
            old_price = exit_order.sell_price

            log.info(
                "... [%s] SELL not filled (check #%d, %.1fs) -- repainting "
                "%s $%.2f -> $%.2f (entry=$%.2f)",
                slot_id, exit_order.check_count, age,
                exit_order.side, old_price, new_sell_price,
                exit_order.entry_price,
            )

            # Re-place the SELL order at the new (possibly better) price
            oid = await self._poly.place_limit_sell(
                exit_order.token_id, new_sell_price, exit_order.size,
            )
            if oid:
                exit_order.order_id = oid
                exit_order.sell_price = new_sell_price
                exit_order.created_at = time.time()
                self._sells_placed += 1
            else:
                log.warning(
                    "!!! [%s] SELL re-place FAILED -- holding to expiry",
                    slot_id,
                )
                self._exit_orders.pop(slot_id, None)
                return "cancelled"

            return "holding"

    # ------------------------------------------------------------------
    # Real order execution
    # ------------------------------------------------------------------

    async def _place_entry(
        self,
        slot_id: str,
        token_id: str,
        side: str,
        price: float,
        size: int,
        tick_idx: int,
    ) -> None:
        """Place entry BUY order on exchange."""
        oid = await self._poly.place_limit_buy(token_id, price, size)
        if oid:
            self._entry_orders[slot_id] = TrackedEntry(
                order_id=oid,
                slot_id=slot_id,
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                tick_idx=tick_idx,
            )
            self._orders_placed += 1
            log.info(
                ">>> [%s] Entry order placed: %s @ $%.2f sz=%d  "
                "(will check fill in %.0fs)",
                slot_id, oid[:16], price, size, MIN_ORDER_LIFETIME_S,
            )
        else:
            log.warning("!!! [%s] Entry order FAILED to place", slot_id)

    # ------------------------------------------------------------------
    # Market rotation handler
    # ------------------------------------------------------------------

    async def handle_rotation(self, slot_id: str) -> None:
        """Cancel pending orders for a slot on market rotation.

        Filled positions are NOT sold -- binary markets auto-settle at
        $0 or $1 on expiry.  We just log and clear tracking.
        """
        # Cancel pending SELL order
        exit_order = self._exit_orders.pop(slot_id, None)
        if exit_order:
            result = await self._poly.cancel_order(exit_order.order_id)
            log.info(
                "~~~ [%s] Rotation: cancelled SELL %s -> %s",
                slot_id, exit_order.order_id[:16], result,
            )
            if result in ("matched", "gone"):
                # SELL was filled before rotation -- count as scalp
                pnl = (exit_order.sell_price - exit_order.entry_price) * exit_order.size
                self._sells_filled += 1
                self._session_trades += 1
                self._session_pnl += pnl
                if pnl > 0:
                    self._session_wins += 1
                log.info(
                    "+++ [%s] SELL filled during rotation! pnl=$%+.4f",
                    slot_id, pnl,
                )

        # Cancel pending entry
        entry = self._entry_orders.pop(slot_id, None)
        if entry:
            result = await self._poly.cancel_order(entry.order_id)
            log.info(
                "~~~ [%s] Rotation: cancelled entry %s -> %s",
                slot_id, entry.order_id[:16], result,
            )
            if result in ("matched", "gone"):
                log.info(
                    "~~~ [%s] Entry was filled before rotation -- "
                    "will auto-settle at expiry",
                    slot_id,
                )

        # Filled positions auto-settle -- just clear tracking
        filled = self._filled_positions.pop(slot_id, None)
        if filled:
            log.info(
                "~~~ [%s] Rotation with filled position (%s @ $%.2f) -- "
                "will auto-settle at expiry",
                slot_id, filled.side, filled.price,
            )

        # Residual shares also auto-settle at expiry
        residual = self._residual.pop(slot_id, None)
        if residual:
            log.info(
                "~~~ [%s] Rotation with %d residual %s shares -- "
                "will auto-settle at expiry",
                slot_id, residual["shares"], residual["side"],
            )

        # Reset engine for this slot
        self._engines[slot_id] = StrategyEngine(self.strat_cfg)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def cancel_all_orders(self) -> None:
        """Cancel pending entry and SELL orders (clean shutdown).

        Filled positions are NOT sold -- binary markets auto-settle.
        """
        # Cancel tracked SELL orders
        for slot_id, exit_order in list(self._exit_orders.items()):
            result = await self._poly.cancel_order(exit_order.order_id)
            log.info("Shutdown: cancelled SELL for %s -> %s", slot_id, result)
        self._exit_orders.clear()

        # Cancel tracked entries
        for slot_id, entry in list(self._entry_orders.items()):
            await self._poly.cancel_order(entry.order_id)
            log.info("Shutdown: cancelled entry for %s", slot_id)
        self._entry_orders.clear()

        # Filled positions auto-settle -- just log
        for slot_id, filled in list(self._filled_positions.items()):
            log.info(
                "Shutdown: filled position for %s (%s @ $%.2f) will auto-settle",
                slot_id, filled.side, filled.price,
            )
        self._filled_positions.clear()

        # Residual shares auto-settle -- just log
        for slot_id, res in list(self._residual.items()):
            log.info(
                "Shutdown: %d residual %s shares for %s will auto-settle",
                res["shares"], res["side"], slot_id,
            )
        self._residual.clear()

        # Cancel any remaining unfilled orders on exchange
        await self._poly.cancel_all()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _write_signal(self, data: dict) -> None:
        if self._signal_file is not None:
            self._signal_file.write(json.dumps(data, default=str) + "\n")
            self._signal_file.flush()

    def _write_trade(self, data: dict) -> None:
        if self._trade_file is not None:
            self._trade_file.write(json.dumps(data, default=str) + "\n")
            self._trade_file.flush()

    def print_summary(self) -> None:
        if self._session_trades == 0:
            pending = len(self._entry_orders)
            filled = len(self._filled_positions)
            selling = len(self._exit_orders)
            log.info(
                "--- LIVE: no trades yet | orders: %d placed, %d filled, "
                "%d cancelled | sells: %d placed, %d filled, %d expired | "
                "pending=%d filled=%d selling=%d",
                self._orders_placed, self._orders_filled, self._orders_cancelled,
                self._sells_placed, self._sells_filled, self._sells_expired,
                pending, filled, selling,
            )
            return
        wr = self._session_wins / self._session_trades * 100

        slot_parts = []
        for sid in sorted(self._slot_trades.keys()):
            cnt = self._slot_trades[sid]
            if cnt > 0:
                slot_parts.append(f"{sid}={cnt}/${self._slot_pnl[sid]:+.2f}")

        pending = len(self._entry_orders)
        filled = len(self._filled_positions)
        selling = len(self._exit_orders)
        log.info(
            "+++ LIVE: %d trades, %.0f%% WR, $%+.2f | "
            "orders: %d placed, %d filled (%.0f%%), %d cancelled | "
            "sells: %d placed, %d filled (%.0f%%), %d expired | "
            "pending=%d filled=%d selling=%d | %s",
            self._session_trades, wr, self._session_pnl,
            self._orders_placed, self._orders_filled,
            (self._orders_filled / self._orders_placed * 100) if self._orders_placed else 0,
            self._orders_cancelled,
            self._sells_placed, self._sells_filled,
            (self._sells_filled / self._sells_placed * 100) if self._sells_placed else 0,
            self._sells_expired,
            pending, filled, selling,
            "  ".join(slot_parts) if slot_parts else "no slot data",
        )

    def close(self) -> None:
        if self._trade_file is not None:
            self._trade_file.close()
        if self._signal_file is not None:
            self._signal_file.close()


# ---------------------------------------------------------------------------
# Async worker
# ---------------------------------------------------------------------------


async def live_trader_worker(
    cfg: Config,
    state: SharedState,
    tick_queue: asyncio.Queue[dict],
) -> None:
    """Async worker: live trading on BTC slots only."""

    strat_cfg = StrategyConfig()

    # Filter to BTC slots only
    btc_slots = [s for s in cfg.market_slots if s.slot_id.startswith("btc-")]
    slot_ids = [s.slot_id for s in btc_slots]

    if not slot_ids:
        log.error("No BTC slots configured -- live trader cannot start")
        return

    # Initialize CLOB client
    poly = PolyClient()
    try:
        poly.connect()
    except Exception:
        log.exception("PolyClient connection FAILED -- live trader cannot start")
        return

    # Cancel any orphan orders from previous session
    log.info("Cancelling orphan orders from previous session...")
    await poly.cancel_all()

    trader = LiveTrader(strat_cfg, slot_ids, poly)

    exit_ticks = strat_cfg.fixed_exit_ticks
    exit_mode = f"scalp +{exit_ticks} ticks ($+{exit_ticks * 0.01:.2f})" if exit_ticks > 0 else "hold-to-expiry"

    log.info(
        "Live trader starting: %d BTC slots [%s]  bankroll=$%.0f  size=%.0f  "
        "max_concurrent=%d  max_exposure=%.0f%%  order_lifetime=%.0fs  "
        "exit=%s  expiry_cancel=%.0fs",
        len(slot_ids), ", ".join(slot_ids),
        strat_cfg.initial_bankroll,
        strat_cfg.base_size_contracts,
        strat_cfg.max_concurrent_positions,
        strat_cfg.max_exposure_frac * 100,
        MIN_ORDER_LIFETIME_S,
        exit_mode,
        EXPIRY_CANCEL_S,
    )

    from latpoly.workers.signal import SlotSignalState, build_normalized_tick

    slot_signal_states: dict[str, SlotSignalState] = {
        sid: SlotSignalState() for sid in slot_ids
    }

    interval = cfg.signal_interval
    summary_interval = 300
    last_summary = 0.0

    # Wait for readiness
    while not state.shutdown.is_set() and not state.ready:
        await asyncio.sleep(0.5)

    if state.shutdown.is_set():
        trader.close()
        return

    log.info(
        "Live trader READY -- placing real orders on %s  [exit: %s]",
        ", ".join(slot_ids), exit_mode,
    )

    consecutive_errors = 0
    max_consecutive_errors = 50

    try:
        while not state.shutdown.is_set():
            t0 = asyncio.get_running_loop().time()

            try:
                for slot_def in btc_slots:
                    sid = slot_def.slot_id
                    bn = state.get_binance(slot_def.binance_symbol)
                    pm = state.get_polymarket(sid)

                    if bn.best_bid is None or pm.yes_best_bid is None:
                        continue

                    ss = slot_signal_states[sid]

                    # Market rotation
                    cid = pm.market.condition_id
                    if cid != ss.last_condition_id:
                        if ss.last_condition_id:
                            # Cancel orders for this slot on rotation
                            await trader.handle_rotation(sid)
                        ss.clear()
                        ss.last_condition_id = cid
                        log.info("Live trader [%s]: market rotated -> %s", sid, cid[:12])

                    # Build normalized tick (same as paper trader)
                    tick = build_normalized_tick(
                        bn, pm,
                        ss.price_history, ss.poly_tracker,
                        ss.zscore_bn_move, ss.zscore_ret1s,
                    )
                    tick["slot_id"] = sid

                    # Feed to engine + execute orders
                    await trader.on_tick(tick, state)

                consecutive_errors = 0

            except asyncio.CancelledError:
                raise
            except Exception:
                consecutive_errors += 1
                if consecutive_errors <= 5 or consecutive_errors % 100 == 0:
                    log.exception(
                        "Live trader tick error (#%d)", consecutive_errors
                    )
                if consecutive_errors >= max_consecutive_errors:
                    log.error(
                        "Live trader: too many errors (%d), stopping",
                        consecutive_errors,
                    )
                    break

            # Periodic summary
            now = time.time()
            if now - last_summary >= summary_interval:
                trader.print_summary()
                last_summary = now

            elapsed = asyncio.get_running_loop().time() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    except asyncio.CancelledError:
        log.info("Live trader cancelled")
    finally:
        log.info("Live trader shutting down -- cancelling all orders...")
        await trader.cancel_all_orders()
        trader.print_summary()
        trader.close()
        log.info(
            "Live trader stopped. Session: %d trades, $%+.2f  "
            "(%d scalps filled, %d held to expiry)",
            trader._session_trades, trader._session_pnl,
            trader._sells_filled, trader._sells_expired,
        )
