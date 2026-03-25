"""W5-live -- Live trading worker with scalp exit (+N ticks).

Mirrors paper_trader_worker but places real orders via Polymarket CLOB API.
Only operates on BTC slots (filters out ETH due to low liquidity).

State machine (11 states):
  IDLE -> ENTRY_PLACING -> ENTRY_WAIT_FILL (3s)
    -> ENTRY_REPRICE (cancel, reposition best_ask, wait 3s; max 3x)
    -> ENTRY_FILLED -> POST_ENTRY_SETTLEMENT (3s on-chain wait)
    -> POSITION_CONFIRM (balance check) -> EXIT_PLACING
    -> EXIT_WAIT_FILL (check every 5s, repaint at best_bid >= entry+2)
    -> TRADE_COMPLETE -> IDLE (no cooldown)

Timing:
  - Entry wait: 3s (MIN_ORDER_LIFETIME_S)
  - Entry reprice: up to 3x (MAX_ENTRY_REPRICE), ~12s max total
  - Settlement wait: 3s (on-chain token settlement)
  - SELL check: every 5s (EXIT_CHECK_INTERVAL_S)
  - Expiry cancel: 7s before market expiry (EXPIRY_CANCEL_S)

SELL rules:
  - Price floor: entry + 2 ticks (guaranteed profit, never sells at loss)
  - Repaint: follows best_bid but never below floor
  - Hangs in book until filled or market expires

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
from latpoly.strategy.engine import Signal, StrategyEngine  # noqa: F401 — Signal used everywhere
from latpoly.strategy.registry import get_strategy

log = logging.getLogger(__name__)

# Polymarket tick size for crypto up/down markets
TICK_SIZE = 0.01

# Minimum seconds an entry order must live before we check fill status.
# Without this, the engine exits on the very next tick (~0.2s) and the
# maker order never has time to fill on the exchange.
MIN_ORDER_LIFETIME_S = float(os.environ.get("LATPOLY_MIN_ORDER_LIFE", "3"))

# Seconds between SELL fill checks (GET order status)
EXIT_CHECK_INTERVAL_S = float(os.environ.get("LATPOLY_EXIT_CHECK_S", "5"))

# Seconds before expiry to cancel SELL and hold to auto-settlement
EXPIRY_CANCEL_S = float(os.environ.get("LATPOLY_EXPIRY_CANCEL_S", "7"))

# Max times to reprice entry BUY before giving up
MAX_ENTRY_REPRICE = int(os.environ.get("LATPOLY_MAX_ENTRY_REPRICE", "3"))


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
    reprice_count: int = 0  # how many times entry was repriced


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
        self._strategy_name = os.environ.get("LATPOLY_STRATEGY", "scalp")
        _engine_cls = get_strategy(self._strategy_name)
        self._engines: dict[str, StrategyEngine] = {
            sid: _engine_cls(strat_cfg) for sid in slot_ids
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

        # SELL failure tracking (backoff to avoid spin loops)
        self._sell_fail_count: dict[str, int] = {}
        self._sell_retry_after: dict[str, float] = {}

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
    # Engine lifecycle
    # ------------------------------------------------------------------

    def _new_engine(self, slot_id: str) -> None:
        """Reset engine for next trade, preserving cycle state if strategy needs it."""
        engine = self._engines.get(slot_id)
        if engine is not None:
            engine.reset_trade()
        else:
            _cls = get_strategy(self._strategy_name)
            self._engines[slot_id] = _cls(self.strat_cfg)

    async def _handle_cycle_done(self, slot_id: str) -> None:
        """Cycle target hit — cancel all open orders and clear tracking."""
        log.info(
            "$$$ [%s] CYCLE DONE — cancelling all open orders for slot",
            slot_id,
        )
        # Cancel pending entry
        entry = self._entry_orders.pop(slot_id, None)
        if entry:
            await self._poly.cancel_order(entry.order_id)
            log.info("$$$ [%s] Cancelled entry order %s", slot_id, entry.order_id[:16])

        # Cancel pending exit (SELL)
        exit_order = self._exit_orders.pop(slot_id, None)
        if exit_order:
            await self._poly.cancel_order(exit_order.order_id)
            log.info("$$$ [%s] Cancelled exit order %s", slot_id, exit_order.order_id[:16])

        # Clear position tracking (tokens auto-settle at expiry)
        self._filled_positions.pop(slot_id, None)
        self._residual.pop(slot_id, None)
        self._sell_fail_count.pop(slot_id, None)
        self._sell_retry_after.pop(slot_id, None)

        # Cancel ALL orders on exchange as safety net
        await self._poly.cancel_all()
        log.info("$$$ [%s] All exchange orders cancelled. Waiting for next market.", slot_id)

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

            if result == "matched":
                # Cancel confirmed order was filled
                filled_size = await self._poly.get_filled_size(pending.order_id)
                if filled_size <= 0:
                    # API didn't return size -- assume full fill
                    filled_size = pending.size

            elif result in ("gone", "failed"):
                # Ambiguous: order might or might not be filled
                # MUST verify via GET before assuming fill
                filled_size = await self._poly.get_filled_size(pending.order_id)
                if filled_size <= 0:
                    # No confirmed fill -- treat as NOT filled
                    self._orders_cancelled += 1
                    log.info(
                        "--- [%s] Entry cancel=%s but NO confirmed fill "
                        "(get_filled_size=0) -- treating as NOT filled. "
                        "Resetting engine.",
                        slot_id, result,
                    )
                    self._new_engine(slot_id)
                    return Signal(action="NONE", side="", reason="entry_unconfirmed")

            if result in ("matched", "gone", "failed") and (
                result == "matched" or filled_size > 0
            ):

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
                        ">>> [%s] STATE: ENTRY_WAIT_FILL -> ENTRY_FILLED "
                        "after %.1fs (cancel=%s) -- %s @ $%.2f sz=%d (of %d ordered)",
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
                    log.info(
                        ">>> [%s] STATE: ENTRY_FILLED -> POST_ENTRY_SETTLEMENT "
                        "(waiting 5s for on-chain settlement)",
                        slot_id,
                    )
                    await asyncio.sleep(5.0)
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
                    self._new_engine(slot_id)
                    # Don't clear filled_positions -- keep tracking
                    # But allow engine to generate new BUY signal
                    self._filled_positions.pop(slot_id, None)
                    return Signal(action="NONE", side="", reason="partial_fill_accumulating")
            elif result == "canceled":
                # Entry NOT filled -- try reprice if under limit
                if pending.reprice_count < MAX_ENTRY_REPRICE:
                    # Reprice: reposition at current best_ask
                    pm = state.get_polymarket(slot_id)
                    if pending.side == "YES":
                        new_price = pm.yes_best_ask or pm.yes_best_bid or pending.price
                    else:
                        new_price = pm.no_best_ask or pm.no_best_bid or pending.price
                    new_price = round(new_price, 2)

                    pending.reprice_count += 1
                    log.info(
                        ">>> [%s] STATE: ENTRY_WAIT_FILL -> ENTRY_REPRICE "
                        "(reprice #%d/%d) $%.2f -> $%.2f",
                        slot_id, pending.reprice_count, MAX_ENTRY_REPRICE,
                        pending.price, new_price,
                    )

                    oid = await self._poly.place_limit_buy(
                        pending.token_id, new_price, pending.size,
                    )
                    if oid:
                        pending.order_id = oid
                        pending.price = new_price
                        pending.created_at = time.time()
                        self._entry_orders[slot_id] = pending
                        self._orders_placed += 1
                        return Signal(action="NONE", side="", reason="entry_repriced")
                    else:
                        log.warning(
                            "!!! [%s] Entry reprice FAILED to place -- giving up",
                            slot_id,
                        )

                # Max reprices exhausted or reprice failed -- give up
                self._orders_cancelled += 1
                log.info(
                    "--- [%s] STATE: ENTRY_WAIT_FILL -> IDLE "
                    "(not filled after %.1fs, %d reprices). Resetting engine.",
                    slot_id, age, pending.reprice_count,
                )
                self._new_engine(slot_id)
                return Signal(action="NONE", side="", reason="entry_cancelled")

        # -- PHASE 2: If we have a filled position with no SELL, skip engine
        if slot_id in self._filled_positions and slot_id not in self._exit_orders:
            # Check backoff -- don't spam SELL retries
            retry_after = self._sell_retry_after.get(slot_id, 0)
            if time.time() < retry_after:
                return Signal(action="NONE", side="", reason="sell_backoff")

            # Never give up -- keep retrying every 30s until market expires
            # (on rotation, handle_rotation will count the loss)
            fails = self._sell_fail_count.get(slot_id, 0)

            filled = self._filled_positions[slot_id]
            log.warning(
                "!!! [%s] Filled position without SELL order -- re-placing "
                "(attempt #%d, retrying every 30s until expiry)",
                slot_id, fails + 1,
            )
            # Wait for on-chain settlement before SELL retry
            await asyncio.sleep(3.0)
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
                # Count as LOSS: wrong-side residual will expire worthless
                res_loss = residual["avg_entry"] * residual["shares"]
                self._session_trades += 1
                self._session_pnl -= res_loss
                self._slot_trades[slot_id] = self._slot_trades.get(slot_id, 0) + 1
                self._slot_pnl[slot_id] = self._slot_pnl.get(slot_id, 0.0) - res_loss
                wr = (
                    self._session_wins / self._session_trades * 100
                    if self._session_trades else 0
                )
                log.warning(
                    "!!! [%s] Residual %d %s shares but signal is %s -- "
                    "LOSS=$-%.2f  [session: %d trades, %.0f%% WR, $%+.2f]",
                    slot_id, residual["shares"], residual["side"], signal.side,
                    res_loss, self._session_trades, wr, self._session_pnl,
                )
                self._residual.pop(slot_id, None)

            token_id = self._get_token_id(state, slot_id, signal.side)
            if not token_id:
                log.warning("[%s] No token_id for %s -- skipping entry", slot_id, signal.side)
                return signal

            # Compute buy size: accounts for residual shares
            buy_size = self._compute_buy_size(slot_id)
            residual_shares = self._get_residual(slot_id)

            # Place at best_ask for aggressive fill (still maker if ask has depth)
            pm = state.get_polymarket(slot_id)
            if signal.side == "YES":
                best_ask = pm.yes_best_ask or signal.entry_price
                best_bid = pm.yes_best_bid or signal.entry_price
            else:
                best_ask = pm.no_best_ask or signal.entry_price
                best_bid = pm.no_best_bid or signal.entry_price
            # Use best_ask for higher fill rate; fallback to bid+1 if ask unavailable
            entry_price = round(best_ask if best_ask else best_bid + TICK_SIZE, 2)

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

        # Check on-chain token balance before SELL (retry up to 3 times)
        balance = 0
        for attempt in range(3):
            balance = await self._poly.get_token_balance(entry.token_id)
            if balance >= 1:
                break
            if attempt < 2:
                log.info(
                    "<<< [%s] Balance=%d (attempt %d/3) "
                    "-- waiting 2s for settlement...",
                    slot_id, balance, attempt + 1,
                )
                await asyncio.sleep(2.0)

        if balance <= 0:
            log.warning(
                "<<< [%s] SELL skipped: on-chain balance=0 after 3 attempts "
                "(tokens not settled). Will retry in 30s.",
                slot_id,
            )
            fails = self._sell_fail_count.get(slot_id, 0) + 1
            self._sell_fail_count[slot_id] = fails
            self._sell_retry_after[slot_id] = time.time() + 30.0
            return

        # Use ACTUAL on-chain balance instead of assumed fill size
        sell_size = min(entry.size, balance)
        # For SELL, accept any balance >= 1 (min_maker only applies to BUY)
        # Polymarket accepts SELL if notional (price × size) >= ~$0.50
        if sell_size < 1:
            log.warning(
                "<<< [%s] SELL skipped: balance=%d < 1 after 5 attempts. "
                "Will retry in 15s.",
                slot_id, sell_size,
            )
            fails = self._sell_fail_count.get(slot_id, 0) + 1
            self._sell_fail_count[slot_id] = fails
            self._sell_retry_after[slot_id] = time.time() + 15.0
            return

        log.info(
            "<<< [%s] Placing SELL: %s @ $%.2f (entry=$%.2f + %d ticks) sz=%d "
            "(balance=%d on-chain)",
            slot_id, entry.side, sell_price, entry.price, exit_ticks, sell_size,
            balance,
        )

        oid = await self._poly.place_limit_sell(
            entry.token_id, sell_price, sell_size,
        )

        if oid:
            self._exit_orders[slot_id] = TrackedExit(
                order_id=oid,
                slot_id=slot_id,
                token_id=entry.token_id,
                side=entry.side,
                entry_price=entry.price,
                sell_price=sell_price,
                size=sell_size,
            )
            self._sells_placed += 1
            self._sell_fail_count.pop(slot_id, None)  # reset fail counter
            log.info(
                "<<< [%s] STATE: POSITION_CONFIRM -> EXIT_WAIT_FILL "
                "SELL %s @ $%.2f sz=%d (check in %.0fs)",
                slot_id, oid[:16], sell_price, entry.size, EXIT_CHECK_INTERVAL_S,
            )
        else:
            # Track consecutive failures -- retry every 30s until market expires
            fails = self._sell_fail_count.get(slot_id, 0) + 1
            self._sell_fail_count[slot_id] = fails
            backoff = 30.0  # fixed 30s between retries
            self._sell_retry_after[slot_id] = time.time() + backoff
            log.warning(
                "!!! [%s] SELL order FAILED to place (attempt #%d) "
                "-- retry in %.0fs (will keep trying until expiry)",
                slot_id, fails, backoff,
            )

    def _compute_sell_price(self, exit_order: TrackedExit, tick: dict) -> float:
        """Compute SELL price: mid + 2 ticks, with decaying floor.

        Uses the current mid-price (avg of best_bid and best_ask) as
        reference, then adds 2 ticks ($0.02) for maker profit.

        Floor starts at entry + exit_ticks (e.g. +$0.02) but decays
        after sell_decay_after_checks repaints to avoid getting stuck.
        Decay: -1 tick every sell_decay_every_checks additional checks.
        Absolute minimum floor = entry price (breakeven, never loss).

        Examples (entry=$0.47, exit_ticks=2, decay_after=10, decay_every=5):
          checks  0-9:  floor = $0.49 (full profit target)
          checks 10-14: floor = $0.48 (reduced by 1 tick)
          checks 15+:   floor = $0.47 (breakeven = entry price)
          Market UP at any time: sell follows mid + 2 ticks (ignores floor)
        """
        prefix = "yes" if exit_order.side == "YES" else "no"
        bid_key = f"pm_{prefix}_best_bid"
        ask_key = f"pm_{prefix}_best_ask"

        best_bid = tick.get(bid_key)
        best_ask = tick.get(ask_key)

        exit_ticks = self.strat_cfg.fixed_exit_ticks

        # Calculate mid price
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask
        else:
            # No market data -- fallback to entry + fixed ticks
            return round(exit_order.entry_price + exit_ticks * TICK_SIZE, 2)

        # Sell at mid + 2 ticks (dynamic target)
        sell_price = _ceil_tick(mid + 2 * TICK_SIZE)

        # --- Decaying floor logic ---
        # Start with full profit floor, reduce after too many failed repaints
        decay_after = self.strat_cfg.sell_decay_after_checks
        decay_every = self.strat_cfg.sell_decay_every_checks

        checks = exit_order.check_count
        if checks <= decay_after:
            # Normal: full profit floor
            floor_ticks = exit_ticks
        else:
            # Decay: reduce floor by 1 tick per decay_every checks
            ticks_lost = (checks - decay_after) // decay_every + 1
            floor_ticks = max(0, exit_ticks - ticks_lost)

        floor_price = round(exit_order.entry_price + floor_ticks * TICK_SIZE, 2)

        # Clamp to valid range
        return max(floor_price, min(0.99, sell_price))

    async def _check_exit_order(self, slot_id: str, tick: dict) -> str:
        """Check if a pending SELL order has been filled (non-destructive).

        Uses GET to check order status — does NOT cancel.
        The SELL order stays in the book the entire time.

        Only cancels the SELL order when:
        - Near expiry (< 7s) -> hold to auto-settle

        Returns:
            "filled"    -- SELL was filled, profit taken
            "holding"   -- SELL still pending in book, keep waiting
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
        if age < EXIT_CHECK_INTERVAL_S:
            return "holding"

        # --- Non-destructive check: GET order status ---
        exit_order.check_count += 1
        order_data = await self._poly.get_order(exit_order.order_id)

        if order_data is None:
            exit_order.created_at = time.time()
            return "holding"

        # Parse fill status from order data
        size_matched_raw = (
            order_data.get("size_matched")
            or order_data.get("matched_size")
            or 0
        )
        try:
            size_matched = int(float(str(size_matched_raw)))
        except (ValueError, TypeError):
            size_matched = 0

        order_status = str(order_data.get("status", "")).lower()

        if size_matched >= exit_order.size or order_status == "matched":
            # FULL FILL
            sold_size = max(size_matched, exit_order.size)
            result = self._record_sell_fill(slot_id, exit_order, sold_size)
            # Check if cycle target was hit → clean up
            engine = self._engines.get(slot_id)
            if engine is not None and getattr(engine, "_status", None) == "DONE":
                await self._handle_cycle_done(slot_id)
            return result

        elif size_matched > 0:
            log.info(
                "... [%s] SELL partial: %d of %d filled (check #%d)",
                slot_id, size_matched, exit_order.size,
                exit_order.check_count,
            )
            exit_order.created_at = time.time()
            return "holding"

        else:
            # NOT FILLED -- repaint to best bid, but ONLY if profitable
            # Minimum sell price = entry + 2 ticks (guaranteed profit)
            min_sell = round(exit_order.entry_price + self.strat_cfg.fixed_exit_ticks * TICK_SIZE, 2)
            prefix = "yes" if exit_order.side == "YES" else "no"
            best_bid = tick.get(f"pm_{prefix}_best_bid")

            if best_bid is not None:
                new_price = round(best_bid, 2)
                # NEVER sell below entry + 2 ticks
                new_price = max(min_sell, min(0.99, new_price))

                if new_price != exit_order.sell_price:
                    # Cancel old SELL and repaint at better price
                    await self._poly.cancel_order(exit_order.order_id)
                    log.info(
                        "<<< [%s] SELL REPAINT: %s $%.2f -> $%.2f (best_bid=$%.2f, floor=$%.2f) "
                        "entry=$%.2f sz=%d",
                        slot_id, exit_order.side, exit_order.sell_price,
                        new_price, best_bid, min_sell,
                        exit_order.entry_price, exit_order.size,
                    )
                    oid = await self._poly.place_limit_sell(
                        exit_order.token_id, new_price, exit_order.size,
                    )
                    if oid:
                        exit_order.order_id = oid
                        exit_order.sell_price = new_price
                        exit_order.created_at = time.time()
                        exit_order.check_count = 0
                    else:
                        log.warning(
                            "!!! [%s] SELL repaint FAILED, retrying next check",
                            slot_id,
                        )
                        exit_order.created_at = time.time()
                else:
                    # Same price -- keep waiting
                    if exit_order.check_count % 10 == 0:
                        log.info(
                            "... [%s] SELL pending (check #%d) -- %s @ $%.2f "
                            "(entry=$%.2f, best_bid=$%.2f, floor=$%.2f)",
                            slot_id, exit_order.check_count,
                            exit_order.side, exit_order.sell_price,
                            exit_order.entry_price, best_bid, min_sell,
                        )
                    exit_order.created_at = time.time()
            else:
                exit_order.created_at = time.time()
            return "holding"

    def _record_sell_fill(
        self, slot_id: str, exit_order: TrackedExit, sold_size: int,
    ) -> str:
        """Record a SELL fill — update stats, log, write trade, reset engine."""
        self._sells_filled += 1
        pnl = (exit_order.sell_price - exit_order.entry_price) * sold_size

        # Check for partial fill residual
        remaining = exit_order.size - sold_size
        if remaining > 0:
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
            "+++ [%s] STATE: EXIT_WAIT_FILL -> TRADE_COMPLETE "
            "%s sold=%d entry=$%.2f sell=$%.2f pnl=$%+.4f  "
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

        # Notify engine of trade result (for cycle PnL tracking in strategy 2+)
        engine = self._engines.get(slot_id)
        if engine is not None:
            engine.notify_trade_result(pnl)

        # Reset engine -- ready for next trade on this slot
        self._new_engine(slot_id)

        return "filled"

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
        log.info(
            ">>> [%s] STATE: IDLE -> ENTRY_PLACING (%s @ $%.2f sz=%d)",
            slot_id, side, price, size,
        )
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
                ">>> [%s] STATE: ENTRY_PLACING -> ENTRY_WAIT_FILL "
                "%s @ $%.2f sz=%d (check in %.0fs)",
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
                engine = self._engines.get(slot_id)
                if engine is not None:
                    engine.notify_trade_result(pnl)
                    if getattr(engine, "_status", None) == "DONE":
                        await self._handle_cycle_done(slot_id)
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

        # Filled positions auto-settle -- count as LOSS (worst case)
        # NOTE: filled_positions and residual track the SAME shares,
        # so only count ONE of them to avoid double-counting.
        filled = self._filled_positions.pop(slot_id, None)
        residual = self._residual.pop(slot_id, None)

        if filled:
            loss = filled.price * filled.size
            self._session_trades += 1
            self._session_pnl -= loss
            self._slot_trades[slot_id] = self._slot_trades.get(slot_id, 0) + 1
            self._slot_pnl[slot_id] = self._slot_pnl.get(slot_id, 0.0) - loss
            log.warning(
                "~~~ [%s] Rotation with filled position (%s @ $%.2f × %d) -- "
                "LOSS=$-%.2f  [session: $%+.2f]",
                slot_id, filled.side, filled.price, filled.size,
                loss, self._session_pnl,
            )
        elif residual:
            # Only count residual if no filled_positions (avoid double-count)
            res_loss = residual["avg_entry"] * residual["shares"]
            self._session_trades += 1
            self._session_pnl -= res_loss
            self._slot_trades[slot_id] = self._slot_trades.get(slot_id, 0) + 1
            self._slot_pnl[slot_id] = self._slot_pnl.get(slot_id, 0.0) - res_loss
            log.warning(
                "~~~ [%s] Rotation with %d residual %s shares -- "
                "LOSS=$-%.2f  [session: $%+.2f]",
                slot_id, residual["shares"], residual["side"],
                res_loss, self._session_pnl,
            )

        # Reset engine for this slot
        self._new_engine(slot_id)

    # ------------------------------------------------------------------
    # Recover orphan positions on startup / rotation
    # ------------------------------------------------------------------

    async def recover_orphan_positions(
        self, slot_id: str, yes_token_id: str, no_token_id: str,
        yes_best_bid: float | None, no_best_bid: float | None,
    ) -> None:
        """Check on-chain balance for both tokens and place SELL for any
        orphaned positions (from restart or prior session).

        Since we don't know the original entry price, we sell at
        best_bid (maximize exit price). The SELL hangs in the book
        until filled or market expires.
        """
        for side, token_id, best_bid in [
            ("YES", yes_token_id, yes_best_bid),
            ("NO", no_token_id, no_best_bid),
        ]:
            # Skip if we already have a SELL or filled position for this slot
            if slot_id in self._exit_orders or slot_id in self._filled_positions:
                continue

            balance = await self._poly.get_token_balance(token_id)
            if balance < self.strat_cfg.min_maker_size:
                continue

            # Found orphan tokens! Place SELL at best_bid
            sell_price = round(best_bid, 2) if best_bid else 0.50
            sell_price = max(0.01, min(0.99, sell_price))

            log.warning(
                ">>> [%s] ORPHAN RECOVERY: found %d %s tokens on-chain. "
                "Placing SELL @ $%.2f (best_bid)",
                slot_id, balance, side, sell_price,
            )

            oid = await self._poly.place_limit_sell(
                token_id, sell_price, balance,
            )
            if oid:
                self._exit_orders[slot_id] = TrackedExit(
                    order_id=oid,
                    slot_id=slot_id,
                    token_id=token_id,
                    side=side,
                    entry_price=sell_price,  # unknown, use sell as proxy
                    sell_price=sell_price,
                    size=balance,
                )
                log.info(
                    "<<< [%s] ORPHAN SELL placed: %s @ $%.2f sz=%d",
                    slot_id, oid[:16], sell_price, balance,
                )
            else:
                log.warning(
                    "!!! [%s] ORPHAN SELL failed to place for %d %s tokens",
                    slot_id, balance, side,
                )

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

    # Filter to allowed live trading slots (env: LATPOLY_LIVE_SLOTS, default: btc-15m)
    allowed_raw = os.environ.get("LATPOLY_LIVE_SLOTS", "btc-15m")
    allowed_ids = {s.strip() for s in allowed_raw.split(",") if s.strip()}
    btc_slots = [s for s in cfg.market_slots if s.slot_id in allowed_ids]
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

    strategy_name = os.environ.get("LATPOLY_STRATEGY", "scalp")
    log.info(
        "Live trader starting: strategy=%s  %d BTC slots [%s]  bankroll=$%.0f  "
        "size=%.0f  max_concurrent=%d  max_exposure=%.0f%%  "
        "order_lifetime=%.0fs  exit=%s  expiry_cancel=%.0fs",
        strategy_name,
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

                        # Pre-approve BOTH tokens for SELL (on-chain tx)
                        # so approval is confirmed before first SELL attempt
                        yes_tok = pm.market.yes_token_id
                        no_tok = pm.market.no_token_id
                        await trader._poly.approve_token(yes_tok)
                        await trader._poly.approve_token(no_tok)

                        # Recover orphan positions from restart/prior session
                        await trader.recover_orphan_positions(
                            sid, yes_tok, no_tok,
                            pm.yes_best_bid, pm.no_best_bid,
                        )

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
