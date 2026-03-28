"""W5-mm — Market Maker trader for Polymarket.

Two-sided quoting with Avellaneda-Stoikov spread/skew.
Uses Binance as quote protection (adverse selection), NOT as entry trigger.
Manages inventory continuously via skew, not binary blocking.

State machine:
  IDLE → QUOTING → INVENTORY_SKEW → EXIT_MODE → HALT → IDLE (rotation)

Activation:
  LATPOLY_TRADING_MODE=mm
  LATPOLY_MM_SLOTS=btc-15m

Config: see MMParams in mm_quote_engine.py for all LATPOLY_MM_* env vars.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from latpoly.config import Config
from latpoly.execution.poly_client import PolyClient
from latpoly.shared_state import SharedState
from latpoly.strategy.mm_quote_engine import (
    MMParams,
    MMQuoteEngine,
    QuotePair,
    TICK_SIZE,
)
from latpoly.strategy.strategy_5 import InformedMMQuoteEngine, S5Params

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_ORDER_AGE_S = 2.0       # Don't check/cancel orders younger than this
EXPIRY_HALT_S = 15.0        # Emergency halt near expiry
MARKET_OPEN_COOLDOWN_S = 15.0  # Don't quote for first 15s of a new market


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TrackedOrder:
    """An open order on Polymarket."""
    order_id: str
    slot_id: str
    token_id: str
    side: str           # "YES" or "NO"
    direction: str      # "BUY" or "SELL"
    price: float
    size: int
    placed_at: float    # time.time()
    last_checked: float = 0.0


@dataclass
class SlotMMState:
    """Per-slot market making state."""

    # Live orders: keys are "bid_yes", "bid_no", "ask_yes", "ask_no"
    orders: dict[str, TrackedOrder] = field(default_factory=dict)

    # Inventory tracking (LOCAL, never from balance API)
    inventory_yes: int = 0
    inventory_no: int = 0
    avg_entry_yes: float = 0.0
    avg_entry_no: float = 0.0

    # Market open cooldown
    cycle_start_time: float = 0.0

    # Phase
    phase: str = "IDLE"  # IDLE | QUOTING | INVENTORY_SKEW | EXIT_MODE | HALT

    # Market identity
    condition_id: str = ""
    yes_token_id: str = ""
    no_token_id: str = ""

    # Rate limiting
    reprices_this_second: int = 0
    reprice_second_start: float = 0.0

    # PnL tracking
    realized_pnl: float = 0.0
    total_fills: int = 0
    total_round_trips: int = 0

    @property
    def net_inventory(self) -> int:
        """Positive = long YES, negative = long NO."""
        return self.inventory_yes - self.inventory_no

    def reset(self) -> None:
        """Full reset for market rotation."""
        self.orders.clear()
        self.inventory_yes = 0
        self.inventory_no = 0
        self.avg_entry_yes = 0.0
        self.avg_entry_no = 0.0
        self.phase = "IDLE"
        self.condition_id = ""
        self.yes_token_id = ""
        self.no_token_id = ""
        self.reprices_this_second = 0
        self.realized_pnl = 0.0
        self.total_fills = 0
        self.total_round_trips = 0
        self.cycle_start_time = time.time()


# ---------------------------------------------------------------------------
# MM Trader
# ---------------------------------------------------------------------------


class MMTrader:
    """Market Maker — two-sided quoting with Avellaneda-Stoikov."""

    def __init__(
        self,
        params: MMParams,
        slot_ids: list[str],
        poly: PolyClient,
        output_dir: str = "data/mm",
        use_s5: bool = False,
    ) -> None:
        self._params = params
        self._poly = poly
        self._use_s5 = use_s5
        if use_s5:
            s5_params = S5Params()
            self._engine = InformedMMQuoteEngine(params, s5_params)
            log.info(
                "[mm_trader] Using Strategy 5 (Informed MM): "
                "trend_strong=%.4f trend_mild=%.4f spread_mult=%.1f "
                "min_profit_ticks=%d trend_block_inv=%d",
                s5_params.trend_strong, s5_params.trend_mild,
                s5_params.trend_spread_mult, s5_params.min_profit_ticks,
                s5_params.trend_block_inv,
            )
        else:
            self._engine = MMQuoteEngine(params)
        self._slots: dict[str, SlotMMState] = {
            sid: SlotMMState() for sid in slot_ids
        }

        # Session stats
        self._session_pnl: float = 0.0
        self._session_fills: int = 0

        # File output
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._trade_file: Optional[object] = None
        self._current_date: str = ""

        log.info(
            "[mm_trader] init: gamma=%.2f base_spread=%d soft_inv=%d "
            "max_inv=%d quote_size=%d adverse_thresh=%.0f slots=%s",
            params.gamma, params.base_spread_ticks,
            params.soft_inventory, params.max_inventory,
            params.quote_size, params.adverse_threshold,
            list(self._slots.keys()),
        )

    # ------------------------------------------------------------------
    # Core tick handler
    # ------------------------------------------------------------------

    async def on_tick(self, tick: dict, state: SharedState) -> None:
        """Main tick handler. Called every ~200ms per slot."""
        slot_id = tick.get("slot_id", "")
        ss = self._slots.get(slot_id)
        if ss is None:
            return

        # --- Market rotation detection ---
        cid = tick.get("condition_id", "")
        if cid and cid != ss.condition_id:
            if ss.condition_id:
                await self._handle_rotation(slot_id)
            ss.condition_id = cid
            # Grab token IDs from Polymarket state
            pm = state.get_polymarket(slot_id)
            if pm and pm.market:
                ss.yes_token_id = pm.market.yes_token_id
                ss.no_token_id = pm.market.no_token_id

        if not ss.yes_token_id or not ss.no_token_id:
            return  # No market discovered yet

        # --- Time phase ---
        tte_ms = tick.get("time_to_expiry_ms")
        if tte_ms is None:
            return
        tte_s = tte_ms / 1000.0

        time_phase = self._engine.compute_time_phase(tte_s)

        # --- State machine transitions ---
        old_phase = ss.phase
        if time_phase == "halt":
            ss.phase = "HALT"
        elif time_phase == "exit":
            ss.phase = "EXIT_MODE"
        elif abs(ss.net_inventory) > self._params.soft_inventory:
            ss.phase = "INVENTORY_SKEW"
        else:
            if ss.phase in ("IDLE", "INVENTORY_SKEW"):
                ss.phase = "QUOTING"

        if ss.phase != old_phase:
            log.info(
                "[mm_trader][%s] phase: %s → %s  inv=%+d (YES=%d NO=%d)  "
                "tte=%.0fs",
                slot_id, old_phase, ss.phase,
                ss.net_inventory, ss.inventory_yes, ss.inventory_no, tte_s,
            )

        # --- HALT: cancel everything, emergency flatten ---
        if ss.phase == "HALT":
            await self._cancel_all_slot(slot_id)
            if ss.net_inventory != 0:
                await self._emergency_flatten(slot_id, state)
            return

        # --- Market open cooldown: don't quote in first 15s ---
        if ss.cycle_start_time > 0:
            elapsed_since_open = time.time() - ss.cycle_start_time
            if elapsed_since_open < MARKET_OPEN_COOLDOWN_S:
                return  # Let data stabilize before quoting

        # --- Check fills on existing orders ---
        await self._check_fills(slot_id)

        # --- Compute desired quotes ---
        if self._use_s5:
            desired = self._engine.compute_quotes(
                tick, ss.net_inventory, time_phase,
                inventory_yes=ss.inventory_yes, inventory_no=ss.inventory_no,
                avg_entry_yes=ss.avg_entry_yes, avg_entry_no=ss.avg_entry_no,
            )
        else:
            desired = self._engine.compute_quotes(
                tick, ss.net_inventory, time_phase,
                inventory_yes=ss.inventory_yes, inventory_no=ss.inventory_no,
            )
        if desired is None:
            return  # Cannot quote (stale data, no mid, etc.)

        # --- Reconcile orders ---
        await self._reconcile_orders(slot_id, desired, tick, state)

    # ------------------------------------------------------------------
    # Fill detection
    # ------------------------------------------------------------------

    async def _check_fills(self, slot_id: str) -> None:
        """Check all open orders for fills (non-destructive GET)."""
        ss = self._slots[slot_id]
        now = time.time()

        filled_keys: list[tuple[str, int]] = []

        for key, order in list(ss.orders.items()):
            # Rate limit checks
            if now - order.last_checked < self._params.fill_check_interval_s:
                continue
            if now - order.placed_at < MIN_ORDER_AGE_S:
                continue

            order.last_checked = now

            try:
                order_info = await self._poly.get_order(order.order_id)
            except Exception as e:
                log.warning("[mm_trader][%s] get_order error: %s", slot_id, e)
                continue

            if order_info is None:
                # Order disappeared (filled or expired)
                filled_keys.append((key, order.size))
                continue

            # Check size_matched field
            raw_matched = order_info.get("size_matched", 0) or 0
            size_matched = int(float(raw_matched))
            status = order_info.get("status", "")

            if status in ("matched", "filled"):
                filled_keys.append((key, order.size))
            elif size_matched >= order.size:
                filled_keys.append((key, order.size))

        # Process fills
        for key, filled_size in filled_keys:
            await self._process_fill(slot_id, key, filled_size)

    async def _process_fill(
        self, slot_id: str, order_key: str, filled_size: int,
    ) -> None:
        """Update local inventory after a fill."""
        ss = self._slots[slot_id]
        order = ss.orders.pop(order_key, None)
        if order is None:
            return

        ss.total_fills += 1
        self._session_fills += 1
        pnl = 0.0

        if order_key == "bid_yes":
            # Bought YES tokens
            old_total = ss.inventory_yes
            new_total = old_total + filled_size
            if new_total > 0:
                ss.avg_entry_yes = (
                    (ss.avg_entry_yes * old_total + order.price * filled_size)
                    / new_total
                )
            ss.inventory_yes = new_total

        elif order_key == "bid_no":
            # Bought NO tokens
            old_total = ss.inventory_no
            new_total = old_total + filled_size
            if new_total > 0:
                ss.avg_entry_no = (
                    (ss.avg_entry_no * old_total + order.price * filled_size)
                    / new_total
                )
            ss.inventory_no = new_total

        elif order_key == "ask_yes":
            # Sold YES tokens
            pnl = (order.price - ss.avg_entry_yes) * filled_size
            ss.inventory_yes = max(0, ss.inventory_yes - filled_size)
            if ss.inventory_yes == 0:
                ss.avg_entry_yes = 0.0
                ss.total_round_trips += 1

        elif order_key == "ask_no":
            # Sold NO tokens
            pnl = (order.price - ss.avg_entry_no) * filled_size
            ss.inventory_no = max(0, ss.inventory_no - filled_size)
            if ss.inventory_no == 0:
                ss.avg_entry_no = 0.0
                ss.total_round_trips += 1

        ss.realized_pnl += pnl
        self._session_pnl += pnl

        log.info(
            "$$$ [mm_trader][%s] FILL %s: %s %s @ $%.2f sz=%d  "
            "pnl=$%+.4f  inv=%+d (YES=%d NO=%d)  "
            "session=$%+.4f  fills=%d  rt=%d",
            slot_id, order_key, order.direction, order.side,
            order.price, filled_size, pnl,
            ss.net_inventory, ss.inventory_yes, ss.inventory_no,
            self._session_pnl, self._session_fills, ss.total_round_trips,
        )

        self._write_fill({
            "ts": time.time(),
            "slot_id": slot_id,
            "order_key": order_key,
            "direction": order.direction,
            "side": order.side,
            "price": order.price,
            "size": filled_size,
            "pnl": round(pnl, 4),
            "net_inventory": ss.net_inventory,
            "inventory_yes": ss.inventory_yes,
            "inventory_no": ss.inventory_no,
            "session_pnl": round(self._session_pnl, 4),
        })

    # ------------------------------------------------------------------
    # Order reconciliation
    # ------------------------------------------------------------------

    async def _reconcile_orders(
        self,
        slot_id: str,
        desired: QuotePair,
        tick: dict,
        state: SharedState,
    ) -> None:
        """Compare desired quotes with live orders. Cancel stale, place new."""
        ss = self._slots[slot_id]
        now = time.time()

        # Reset rate limit counter each second
        if now - ss.reprice_second_start >= 1.0:
            ss.reprices_this_second = 0
            ss.reprice_second_start = now

        # Reconcile each order slot
        await self._reconcile_one(
            slot_id, "bid_yes",
            ss.yes_token_id, "YES", "BUY",
            desired.bid_yes_price, desired.bid_yes_size,
        )
        await self._reconcile_one(
            slot_id, "bid_no",
            ss.no_token_id, "NO", "BUY",
            desired.bid_no_price, desired.bid_no_size,
        )

        # If we have YES inventory, place SELL YES
        if ss.inventory_yes > 0 and desired.bid_no_size > 0:
            # bid_no = ASK YES in disguise, but we can also place direct SELL YES
            # for inventory we already hold
            ask_yes_price = round(1.0 - desired.bid_no_price, 2)
            await self._reconcile_one(
                slot_id, "ask_yes",
                ss.yes_token_id, "YES", "SELL",
                ask_yes_price, min(desired.bid_no_size, ss.inventory_yes),
            )
        elif "ask_yes" in ss.orders:
            await self._cancel_order(slot_id, "ask_yes")

        # If we have NO inventory, place SELL NO
        if ss.inventory_no > 0 and desired.bid_yes_size > 0:
            ask_no_price = round(1.0 - desired.bid_yes_price, 2)
            await self._reconcile_one(
                slot_id, "ask_no",
                ss.no_token_id, "NO", "SELL",
                ask_no_price, min(desired.bid_yes_size, ss.inventory_no),
            )
        elif "ask_no" in ss.orders:
            await self._cancel_order(slot_id, "ask_no")

    async def _reconcile_one(
        self,
        slot_id: str,
        order_key: str,
        token_id: str,
        side: str,
        direction: str,
        desired_price: float,
        desired_size: int,
    ) -> None:
        """Reconcile a single order slot."""
        ss = self._slots[slot_id]
        existing = ss.orders.get(order_key)

        # Case 1: Want 0 size, have order → cancel
        if desired_size <= 0:
            if existing:
                await self._cancel_order(slot_id, order_key)
            return

        # Case 2: Want size > 0, no existing order → place new
        if existing is None:
            await self._place_order(
                slot_id, order_key, token_id, side, direction,
                desired_price, desired_size,
            )
            return

        # Case 3: Existing order, check if price changed
        price_diff = abs(existing.price - desired_price)
        if price_diff < TICK_SIZE * 0.5:
            return  # Same price, keep it

        # Price changed — need cancel-replace
        # Check rate limit
        if ss.reprices_this_second >= self._params.max_reprices_per_s:
            return  # Too many reprices, leave stale quote

        # Don't reprice orders that are too young
        if time.time() - existing.placed_at < MIN_ORDER_AGE_S:
            return

        ss.reprices_this_second += 1

        # Cancel existing
        result = await self._cancel_order(slot_id, order_key)

        # If cancel returned "matched" → fill was processed in _cancel_order
        if result == "matched":
            # Order filled between our check and cancel — don't place new
            # Inventory was updated in _cancel_order
            return

        # Place new order at desired price
        await self._place_order(
            slot_id, order_key, token_id, side, direction,
            desired_price, desired_size,
        )

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def _place_order(
        self,
        slot_id: str,
        order_key: str,
        token_id: str,
        side: str,
        direction: str,
        price: float,
        size: int,
    ) -> Optional[str]:
        """Place a single order on Polymarket."""
        if size < self._params.min_maker_size:
            return None

        # Polymarket minimum order value is $1.00
        if price * size < 1.0:
            return None

        try:
            if direction == "BUY":
                oid = await self._poly.place_limit_buy(token_id, price, size)
            else:
                oid = await self._poly.place_limit_sell(token_id, price, size)
        except Exception as e:
            log.warning(
                "[mm_trader][%s] place %s failed: %s",
                slot_id, order_key, e,
            )
            return None

        if oid:
            self._slots[slot_id].orders[order_key] = TrackedOrder(
                order_id=oid,
                slot_id=slot_id,
                token_id=token_id,
                side=side,
                direction=direction,
                price=price,
                size=size,
                placed_at=time.time(),
            )
            log.debug(
                "[mm_trader][%s] placed %s: %s %s @ $%.2f sz=%d",
                slot_id, order_key, direction, side, price, size,
            )
        return oid

    async def _cancel_order(self, slot_id: str, order_key: str) -> str:
        """Cancel an order. If result is 'matched', process as fill."""
        ss = self._slots[slot_id]
        order = ss.orders.get(order_key)
        if order is None:
            return "gone"

        try:
            result = await self._poly.cancel_order(order.order_id)
        except Exception as e:
            log.warning(
                "[mm_trader][%s] cancel %s failed: %s",
                slot_id, order_key, e,
            )
            # Remove from tracking to avoid stale references
            ss.orders.pop(order_key, None)
            return "failed"

        if result in ("matched", "gone"):
            # Order was filled before cancel arrived
            await self._process_fill(slot_id, order_key, order.size)
        else:
            # Successfully cancelled
            ss.orders.pop(order_key, None)

        return result

    # ------------------------------------------------------------------
    # Cancel all orders for a slot
    # ------------------------------------------------------------------

    async def _cancel_all_slot(self, slot_id: str) -> None:
        """Cancel all orders for a slot."""
        ss = self._slots[slot_id]
        for key in list(ss.orders.keys()):
            await self._cancel_order(slot_id, key)

    # ------------------------------------------------------------------
    # Market rotation
    # ------------------------------------------------------------------

    async def _handle_rotation(self, slot_id: str) -> None:
        """Cancel all orders, settle inventory, reset state."""
        ss = self._slots[slot_id]
        old_pnl = ss.realized_pnl
        old_fills = ss.total_fills
        old_inv = ss.net_inventory

        # Cancel all live orders (process fills if any)
        await self._cancel_all_slot(slot_id)

        # Remaining inventory settles at expiry (binary outcome)
        # Count as LOSS (worst case: all shares worth $0)
        if ss.inventory_yes > 0:
            loss = ss.avg_entry_yes * ss.inventory_yes
            ss.realized_pnl -= loss
            self._session_pnl -= loss
            log.warning(
                "~~~ [mm_trader][%s] rotation: %d YES shares unsold "
                "→ LOSS=$-%.2f",
                slot_id, ss.inventory_yes, loss,
            )
        if ss.inventory_no > 0:
            loss = ss.avg_entry_no * ss.inventory_no
            ss.realized_pnl -= loss
            self._session_pnl -= loss
            log.warning(
                "~~~ [mm_trader][%s] rotation: %d NO shares unsold "
                "→ LOSS=$-%.2f",
                slot_id, ss.inventory_no, loss,
            )

        log.info(
            "$$$ [mm_trader][%s] CYCLE END: pnl=$%+.4f fills=%d "
            "round_trips=%d  final_inv=%+d  session=$%+.4f",
            slot_id, ss.realized_pnl, ss.total_fills,
            ss.total_round_trips, old_inv, self._session_pnl,
        )

        ss.reset()

    # ------------------------------------------------------------------
    # Emergency flatten
    # ------------------------------------------------------------------

    async def _emergency_flatten(
        self, slot_id: str, state: SharedState,
    ) -> None:
        """HALT state: exit ALL inventory at ANY price.

        A partial loss is ALWAYS better than 100% loss at expiry.
        Previously this only sold at profit — causing 11-share positions
        to expire worthless every cycle.
        """
        ss = self._slots[slot_id]
        pm = state.get_polymarket(slot_id)
        if pm is None:
            return

        # Try to sell YES shares — at ANY price (not just profit)
        if ss.inventory_yes > 0 and ss.yes_token_id:
            best_bid = pm.yes_best_bid
            if best_bid is not None and best_bid > 0.01:
                try:
                    oid = await self._poly.place_market_sell(
                        ss.yes_token_id, best_bid, ss.inventory_yes,
                    )
                    if oid:
                        pnl = (best_bid - ss.avg_entry_yes) * ss.inventory_yes
                        ss.realized_pnl += pnl
                        self._session_pnl += pnl
                        log.info(
                            "[mm_trader][%s] EMERGENCY SELL YES: %d @ $%.2f "
                            "pnl=$%+.4f (avg_entry=$%.2f)",
                            slot_id, ss.inventory_yes, best_bid, pnl,
                            ss.avg_entry_yes,
                        )
                        ss.inventory_yes = 0
                        ss.avg_entry_yes = 0.0
                except Exception as e:
                    log.warning(
                        "[mm_trader][%s] emergency sell YES failed: %s",
                        slot_id, e,
                    )

        # Try to sell NO shares — at ANY price
        if ss.inventory_no > 0 and ss.no_token_id:
            best_bid = pm.no_best_bid
            if best_bid is not None and best_bid > 0.01:
                try:
                    oid = await self._poly.place_market_sell(
                        ss.no_token_id, best_bid, ss.inventory_no,
                    )
                    if oid:
                        pnl = (best_bid - ss.avg_entry_no) * ss.inventory_no
                        ss.realized_pnl += pnl
                        self._session_pnl += pnl
                        log.info(
                            "[mm_trader][%s] EMERGENCY SELL NO: %d @ $%.2f "
                            "pnl=$%+.4f (avg_entry=$%.2f)",
                            slot_id, ss.inventory_no, best_bid, pnl,
                            ss.avg_entry_no,
                        )
                        ss.inventory_no = 0
                        ss.avg_entry_no = 0.0
                except Exception as e:
                    log.warning(
                        "[mm_trader][%s] emergency sell NO failed: %s",
                        slot_id, e,
                    )

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _write_fill(self, record: dict) -> None:
        """Write fill record to JSONL file."""
        import orjson
        self._rotate_files()
        if self._trade_file:
            self._trade_file.write(orjson.dumps(record))
            self._trade_file.write(b"\n")
            self._trade_file.flush()

    def _rotate_files(self) -> None:
        """Open new file per UTC date."""
        import datetime
        today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        if today != self._current_date:
            if self._trade_file:
                self._trade_file.close()
            fpath = self._output_dir / f"mm_fills_{today}.jsonl"
            self._trade_file = open(fpath, "ab")
            self._current_date = today

    def print_summary(self) -> None:
        """Log session summary."""
        log.info(
            "=== [mm_trader] SESSION SUMMARY: pnl=$%+.4f fills=%d ===",
            self._session_pnl, self._session_fills,
        )
        for sid, ss in self._slots.items():
            log.info(
                "  [%s] pnl=$%+.4f fills=%d round_trips=%d",
                sid, ss.realized_pnl, ss.total_fills, ss.total_round_trips,
            )

    def close(self) -> None:
        """Clean up resources."""
        self.print_summary()
        if self._trade_file:
            self._trade_file.close()
            self._trade_file = None


# ---------------------------------------------------------------------------
# Worker coroutine
# ---------------------------------------------------------------------------


async def mm_trader_worker(
    cfg: Config,
    state: SharedState,
    writer_queue: asyncio.Queue[dict],
) -> None:
    """Async worker: market making on configured slots."""

    params = MMParams()

    # Strategy 5 (Informed MM) toggle
    use_s5 = os.environ.get("LATPOLY_MM_STRATEGY", "").strip().lower() in ("s5", "strategy_5", "informed")

    # Filter to allowed MM slots
    allowed_raw = os.environ.get("LATPOLY_MM_SLOTS", "btc-15m")
    allowed_ids = {s.strip() for s in allowed_raw.split(",") if s.strip()}
    mm_slots = [s for s in cfg.market_slots if s.slot_id in allowed_ids]

    if not mm_slots:
        log.error("[mm_trader] No valid MM slots found. Available: %s",
                  [s.slot_id for s in cfg.market_slots])
        return

    slot_ids = [s.slot_id for s in mm_slots]
    log.info("[mm_trader] Starting MM on slots: %s  strategy=%s",
             slot_ids, "S5-informed" if use_s5 else "base-AS")

    # Initialize PolyClient
    poly = PolyClient()
    poly.connect()
    await poly.cancel_all()  # clean orphan orders from prior session

    trader = MMTrader(params, slot_ids, poly, use_s5=use_s5)

    # Reuse signal infrastructure for tick building
    from latpoly.workers.signal import (
        SlotSignalState,
        build_normalized_tick,
    )
    slot_signal_states = {sid: SlotSignalState() for sid in slot_ids}

    # Wait for data readiness
    while not state.shutdown.is_set() and not state.ready:
        await asyncio.sleep(0.5)

    if state.shutdown.is_set():
        trader.close()
        return

    log.info("[mm_trader] Data ready. Starting quoting loop.")

    interval = cfg.signal_interval  # 200ms
    loop = asyncio.get_running_loop()

    try:
        while not state.shutdown.is_set():
            t0 = loop.time()

            for slot_def in mm_slots:
                sid = slot_def.slot_id
                sym = slot_def.binance_symbol

                bn = state.get_binance(sym)
                pm = state.get_polymarket(sid)
                if bn is None or pm is None:
                    continue
                if bn.best_bid is None or pm.yes_best_bid is None:
                    continue

                ss = slot_signal_states[sid]
                cid = pm.market.condition_id if pm.market else ""

                # Market rotation for signal state
                if cid and cid != ss.last_condition_id:
                    if ss.last_condition_id:
                        ss.clear()
                    ss.last_condition_id = cid

                # Build normalized tick (reuse signal.py infra)
                tick = build_normalized_tick(
                    bn, pm,
                    ss.price_history, ss.poly_tracker,
                    ss.zscore_bn_move, ss.zscore_ret1s,
                )
                tick["slot_id"] = sid

                # Feed to MM trader
                try:
                    await trader.on_tick(tick, state)
                except Exception as e:
                    log.error(
                        "[mm_trader][%s] on_tick error: %s: %s",
                        sid, type(e).__name__, e,
                    )

            elapsed = loop.time() - t0
            sleep_time = max(0.0, interval - elapsed)
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        log.info("[mm_trader] Cancelled, shutting down...")
    except Exception as e:
        log.error("[mm_trader] Fatal error: %s: %s", type(e).__name__, e)
    finally:
        # Emergency cleanup: cancel all orders
        try:
            await poly.cancel_all()
        except Exception:
            pass
        trader.close()
