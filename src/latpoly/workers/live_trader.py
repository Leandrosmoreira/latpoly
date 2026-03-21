"""W5-live — Live trading worker with real order execution.

Mirrors paper_trader_worker but places real orders via Polymarket CLOB API.
Only operates on BTC slots (filters out ETH due to low liquidity).

Order lifecycle:
1. Engine BUY signal  -> place GTC limit BUY at best_bid + 1 tick (maker, queue priority)
2. Freeze slot for MIN_ORDER_LIFETIME_S (~7s, don't feed engine, let order fill)
3. After freeze, cancel order to check fill status:
   - "canceled"        -> entry never filled (~60-70%), engine resets, done
   - "matched"/"gone"  -> entry filled (~30-40%), hold to expiry
4. Filled positions are HELD to expiry — binary markets auto-settle at $0 or $1
5. Market rotation     -> cancel pending entries, filled positions auto-settle
6. Shutdown            -> cancel pending entries, filled positions auto-settle

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
# Tracked order
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


# ---------------------------------------------------------------------------
# Live Trader
# ---------------------------------------------------------------------------


class LiveTrader:
    """Live trading wrapper — places real orders based on engine signals.

    Uses the same StrategyEngine as paper_trader. The engine tracks virtual
    positions; this class shadows them with real Polymarket orders.

    Key design: when an entry order is placed, we FREEZE the slot (don't
    feed ticks to the engine) for MIN_ORDER_LIFETIME_S seconds. This gives
    the maker order time to fill. After the freeze, we check fill status
    and either resume the engine (if filled) or reset (if not filled).
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

        # Background tasks for async order placement
        self._pending_tasks: list[asyncio.Task] = []

        # Session stats
        self._session_trades: int = 0
        self._session_pnl: float = 0.0
        self._session_wins: int = 0
        self._orders_placed: int = 0
        self._orders_filled: int = 0
        self._orders_cancelled: int = 0

        # Per-slot stats
        self._slot_trades: dict[str, int] = {sid: 0 for sid in slot_ids}
        self._slot_pnl: dict[str, float] = {sid: 0.0 for sid in slot_ids}

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
        """Process one tick: run engine, execute real orders on signals."""
        self._rotate_files()

        slot_id = tick.get("slot_id", "unknown")
        engine = self._engines.get(slot_id)
        if engine is None:
            return Signal(action="NONE", side="", reason="unknown_slot")

        self._tick_idx[slot_id] = self._tick_idx.get(slot_id, 0) + 1
        tick_idx = self._tick_idx[slot_id]

        # ── PHASE 1: Check pending entry orders ──────────────────────
        pending = self._entry_orders.get(slot_id)
        if pending is not None:
            age = time.time() - pending.created_at
            if age < MIN_ORDER_LIFETIME_S:
                # Order still young — don't feed engine, let it fill
                return Signal(action="NONE", side="", reason="order_pending")

            # Order is old enough — check fill status via cancel
            result = await self._poly.cancel_order(pending.order_id)
            self._entry_orders.pop(slot_id, None)

            if result in ("matched", "gone", "failed"):
                # Entry was FILLED — we have real shares now
                self._orders_filled += 1
                self._filled_positions[slot_id] = pending
                log.info(
                    "📥 [%s] Entry FILLED after %.1fs (cancel=%s) — "
                    "holding %s @ $%.2f sz=%d",
                    slot_id, age, result,
                    pending.side, pending.price, pending.size,
                )
            else:
                # Entry NOT filled — cancel succeeded
                self._orders_cancelled += 1
                log.info(
                    "📥 [%s] Entry NOT filled after %.1fs — cancelled. "
                    "Resetting engine.",
                    slot_id, age,
                )
                # Reset the engine so it doesn't think it has a position
                self._engines[slot_id] = StrategyEngine(self.strat_cfg)
                return Signal(action="NONE", side="", reason="entry_cancelled")

        # ── PHASE 2: Run strategy engine ─────────────────────────────
        signal = engine.on_tick(tick, tick_idx)

        if signal.action == "NONE":
            return signal

        now_iso = time.strftime("%H:%M:%S", time.gmtime())

        # ── PHASE 3: Handle ENTRY signals ────────────────────────────
        if signal.action in ("BUY_YES", "BUY_NO"):
            # Don't enter if we already have a filled position on this slot
            if slot_id in self._filled_positions:
                return Signal(action="NONE", side="", reason="already_filled")

            token_id = self._get_token_id(state, slot_id, signal.side)
            if not token_id:
                log.warning("[%s] No token_id for %s — skipping entry", slot_id, signal.side)
                return signal

            # Place 1 tick above best_bid for queue priority
            pm = state.get_polymarket(slot_id)
            if signal.side == "YES":
                best_bid = pm.yes_best_bid or signal.entry_price
            else:
                best_bid = pm.no_best_bid or signal.entry_price
            entry_price = round(best_bid + TICK_SIZE, 2)

            log.info(
                "\U0001f534 [%s] LIVE ENTRY: %s %s sz=%d @ $%.2f -> target=$%.4f  [%s]",
                slot_id, signal.action, signal.side, signal.size,
                entry_price, signal.exit_target, signal.reason,
            )

            self._write_signal({
                "ts": now_iso,
                "slot_id": slot_id,
                "action": signal.action,
                "side": signal.side,
                "entry_price": entry_price,
                "exit_target": signal.exit_target,
                "size": signal.size,
                "reason": signal.reason,
                "token_id": token_id[:16],
                "tick_idx": tick_idx,
            })

            # Place real BUY order (await to ensure it's tracked)
            await self._place_entry(
                slot_id, token_id, signal.side,
                entry_price, signal.size, tick_idx,
            )

        # ── PHASE 4: Handle EXIT signals ─────────────────────────────
        elif signal.action == "EXIT":
            trades = engine.closed_trades
            if trades:
                last_trade = trades[-1]
                pnl = last_trade["pnl_net"]
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

                emoji = "\u2705" if pnl > 0 else "\u274c"
                log.info(
                    "%s [%s] LIVE EXIT: %s %s entry=$%.4f exit=$%.4f pnl=$%+.4f  "
                    "hold=%dt  type=%s  [session: %d trades, %.0f%% WR, $%+.2f]",
                    emoji, slot_id, signal.side, signal.reason,
                    last_trade["entry_price"], last_trade["exit_price"], pnl,
                    last_trade["hold_ticks"], last_trade["exit_type"],
                    self._session_trades, wr, self._session_pnl,
                )

                self._write_trade({
                    "ts": now_iso,
                    "slot_id": slot_id,
                    **last_trade,
                    "session_pnl": round(self._session_pnl, 4),
                    "session_trades": self._session_trades,
                    "session_win_rate": round(wr, 1),
                })

                # Hold to expiry — binary markets auto-settle at $0 or $1
                filled = self._filled_positions.get(slot_id)
                if filled is not None:
                    log.info(
                        "🏁 [%s] Holding to expiry (no SELL): %s @ $%.2f sz=%d — "
                        "market will auto-settle at $0 or $1",
                        slot_id, filled.side, filled.price, filled.size,
                    )
                    # Don't pop — keep tracking until rotation clears it
                else:
                    log.info(
                        "📥 [%s] Engine EXIT but no filled position (virtual only)",
                        slot_id,
                    )

        elif signal.action == "HOLD_TO_EXPIRY":
            log.info(
                "\U0001f3c1 [%s] LIVE HOLD_TO_EXPIRY: %s -- %s",
                slot_id, signal.side, signal.reason,
            )

        # Clean up completed tasks
        self._pending_tasks = [t for t in self._pending_tasks if not t.done()]

        return signal

    # ------------------------------------------------------------------
    # Exit price computation
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_real_exit_price(trade: dict) -> float:
        """Compute exchange-compatible exit price (rounded to tick)."""
        exit_type = trade.get("exit_type", "")
        entry = trade["entry_price"]
        target = trade["exit_price"]

        if exit_type == "MAKER":
            # Target hit: sell at ceil(exit_target) but at least entry + 1 tick
            sell_px = _ceil_tick(target)
            return max(sell_px, entry + TICK_SIZE)

        elif exit_type in ("STOP_LOSS", "TIMEOUT"):
            # Urgent: sell at floor(bid) for aggressive fill
            return _floor_tick(target)

        else:
            # Settlement / expiry — Polymarket settles automatically
            return _floor_tick(target)

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
                "\U0001f4e4 [%s] Entry order placed: %s @ $%.2f sz=%d  "
                "(will check fill in %.0fs)",
                slot_id, oid[:16], price, size, MIN_ORDER_LIFETIME_S,
            )
        else:
            log.warning("\U0001f4e4 [%s] Entry order FAILED to place", slot_id)

    # ------------------------------------------------------------------
    # Market rotation handler
    # ------------------------------------------------------------------

    async def handle_rotation(self, slot_id: str) -> None:
        """Cancel pending orders for a slot on market rotation.

        Filled positions are NOT sold — binary markets auto-settle at
        $0 or $1 on expiry.  We just log and clear tracking.
        """
        # Cancel pending entry
        entry = self._entry_orders.pop(slot_id, None)
        if entry:
            result = await self._poly.cancel_order(entry.order_id)
            log.info(
                "\U0001f504 [%s] Rotation: cancelled entry %s -> %s",
                slot_id, entry.order_id[:16], result,
            )
            if result in ("matched", "gone"):
                log.info(
                    "\U0001f504 [%s] Entry was filled before rotation — "
                    "will auto-settle at expiry",
                    slot_id,
                )

        # Filled positions auto-settle — just clear tracking
        filled = self._filled_positions.pop(slot_id, None)
        if filled:
            log.info(
                "\U0001f504 [%s] Rotation with filled position (%s @ $%.2f) — "
                "will auto-settle at expiry",
                slot_id, filled.side, filled.price,
            )

        # Reset engine for this slot
        self._engines[slot_id] = StrategyEngine(self.strat_cfg)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def cancel_all_orders(self) -> None:
        """Cancel pending entry orders (clean shutdown).

        Filled positions are NOT sold — binary markets auto-settle.
        """
        # Cancel tracked entries
        for slot_id, entry in list(self._entry_orders.items()):
            await self._poly.cancel_order(entry.order_id)
            log.info("Shutdown: cancelled entry for %s", slot_id)
        self._entry_orders.clear()

        # Filled positions auto-settle — just log
        for slot_id, filled in list(self._filled_positions.items()):
            log.info(
                "Shutdown: filled position for %s (%s @ $%.2f) will auto-settle",
                slot_id, filled.side, filled.price,
            )
        self._filled_positions.clear()

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
            log.info(
                "\U0001f4ca LIVE: no trades yet | orders: %d placed, %d filled, "
                "%d cancelled | pending=%d filled_pos=%d",
                self._orders_placed, self._orders_filled, self._orders_cancelled,
                pending, filled,
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
        log.info(
            "\U0001f4ca LIVE: %d trades, %.0f%% WR, $%+.2f | "
            "orders: %d placed, %d filled (%.0f%%), %d cancelled | "
            "pending=%d filled_pos=%d | %s",
            self._session_trades, wr, self._session_pnl,
            self._orders_placed, self._orders_filled,
            (self._orders_filled / self._orders_placed * 100) if self._orders_placed else 0,
            self._orders_cancelled,
            pending, filled,
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
        log.error("No BTC slots configured — live trader cannot start")
        return

    # Initialize CLOB client
    poly = PolyClient()
    try:
        poly.connect()
    except Exception:
        log.exception("PolyClient connection FAILED — live trader cannot start")
        return

    # Cancel any orphan orders from previous session
    log.info("Cancelling orphan orders from previous session...")
    await poly.cancel_all()

    trader = LiveTrader(strat_cfg, slot_ids, poly)

    log.info(
        "Live trader starting: %d BTC slots [%s]  bankroll=$%.0f  size=%.0f  "
        "max_concurrent=%d  max_exposure=%.0f%%  order_lifetime=%.0fs",
        len(slot_ids), ", ".join(slot_ids),
        strat_cfg.initial_bankroll,
        strat_cfg.base_size_contracts,
        strat_cfg.max_concurrent_positions,
        strat_cfg.max_exposure_frac * 100,
        MIN_ORDER_LIFETIME_S,
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
        "Live trader READY — placing real orders on %s", ", ".join(slot_ids)
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
        log.info("Live trader shutting down — cancelling all orders...")
        await trader.cancel_all_orders()
        trader.print_summary()
        trader.close()
        log.info(
            "Live trader stopped. Session: %d trades, $%+.2f",
            trader._session_trades, trader._session_pnl,
        )
