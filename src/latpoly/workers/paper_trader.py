"""W5 — Paper trading worker: runs StrategyEngine on live ticks (no real orders).

Multi-market: one StrategyEngine per slot, with global risk management.
Phase 3.1 — validates the strategy in real-time before going live.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import IO, Optional

from latpoly.config import Config
from latpoly.shared_state import SharedState
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import StrategyEngine, Signal  # noqa: F401
from latpoly.strategy.registry import get_strategy

log = logging.getLogger(__name__)


class PaperTrader:
    """Wraps multiple StrategyEngines (one per slot) with logging and global risk."""

    def __init__(self, strat_cfg: StrategyConfig, slot_ids: list[str],
                 output_dir: str = "data/paper") -> None:
        self.strat_cfg = strat_cfg
        _engine_cls = get_strategy(os.environ.get("LATPOLY_STRATEGY", "scalp"))
        self._engines: dict[str, StrategyEngine] = {
            sid: _engine_cls(strat_cfg) for sid in slot_ids
        }
        self._tick_idx: dict[str, int] = {sid: 0 for sid in slot_ids}
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._trade_file: Optional[IO] = None
        self._signal_file: Optional[IO] = None
        self._current_date: str = ""

        # Per-slot daily stats
        self._daily_signals: int = 0
        self._daily_entries: int = 0
        self._daily_exits: int = 0
        self._daily_pnl: float = 0.0

        # Global session stats
        self._session_pnl: float = 0.0
        self._session_trades: int = 0
        self._session_wins: int = 0

        # Per-slot session P&L (for display)
        self._slot_pnl: dict[str, float] = {sid: 0.0 for sid in slot_ids}
        self._slot_trades: dict[str, int] = {sid: 0 for sid in slot_ids}

    def _rotate_files(self) -> None:
        """Open new output files if the date changed."""
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today == self._current_date:
            return

        if self._trade_file is not None:
            self._trade_file.close()
        if self._signal_file is not None:
            self._signal_file.close()

        self._current_date = today
        trade_path = self._output_dir / f"paper_trades_{today}.jsonl"
        signal_path = self._output_dir / f"paper_signals_{today}.jsonl"

        try:
            self._trade_file = open(trade_path, "a")
            self._signal_file = open(signal_path, "a")
        except OSError:
            log.exception("Paper trader: cannot open output files")
            return
        log.info("Paper trader: writing to %s", trade_path)

        # Reset daily stats
        self._daily_signals = 0
        self._daily_entries = 0
        self._daily_exits = 0
        self._daily_pnl = 0.0

    def _check_global_risk(self) -> bool:
        """Return True if trading is allowed (global kill switch not hit)."""
        total_pnl = sum(
            sum(t["pnl_net"] for t in e.closed_trades)
            for e in self._engines.values()
        )
        if total_pnl <= -self.strat_cfg.max_daily_loss:
            return False
        return True

    def on_tick(self, tick: dict) -> Signal:
        """Process one live tick through the appropriate slot engine."""
        self._rotate_files()

        slot_id = tick.get("slot_id", "unknown")
        engine = self._engines.get(slot_id)
        if engine is None:
            return Signal(action="NONE", side="", reason="unknown_slot")

        self._tick_idx[slot_id] = self._tick_idx.get(slot_id, 0) + 1
        tick_idx = self._tick_idx[slot_id]

        # Global risk check before entry
        if not self._check_global_risk():
            # Still allow exits, but skip entry signals
            if engine._position is None:
                return Signal(action="NONE", side="", reason="global_risk")

        signal = engine.on_tick(tick, tick_idx)

        if signal.action == "NONE":
            return signal

        self._daily_signals += 1
        now_iso = time.strftime("%H:%M:%S", time.gmtime())

        if signal.action in ("BUY_YES", "BUY_NO"):
            self._daily_entries += 1
            log.info(
                "📝 [%s] PAPER ENTRY: %s %s sz=%d @ $%.4f → target=$%.4f  edge=%.4f  [%s]",
                slot_id, signal.action, signal.side, signal.size,
                signal.entry_price, signal.exit_target,
                signal.net_edge, signal.reason,
            )
            self._write_signal({
                "ts": now_iso,
                "ts_ns": tick.get("ts_ns"),
                "slot_id": slot_id,
                "action": signal.action,
                "side": signal.side,
                "entry_price": signal.entry_price,
                "exit_target": signal.exit_target,
                "size": signal.size,
                "net_edge": round(signal.net_edge, 6),
                "time_weight": round(signal.time_weight, 4),
                "reason": signal.reason,
                "tick_idx": tick_idx,
                "condition_id": tick.get("condition_id"),
                "mid_binance": tick.get("mid_binance"),
                "distance_to_strike": tick.get("distance_to_strike"),
                "tte_s": (tick.get("time_to_expiry_ms") or 0) / 1000.0,
            })

        elif signal.action == "EXIT":
            self._daily_exits += 1

            trades = engine.closed_trades
            if trades:
                last_trade = trades[-1]
                pnl = last_trade["pnl_net"]
                self._daily_pnl += pnl
                self._session_pnl += pnl
                self._session_trades += 1
                self._slot_pnl[slot_id] = self._slot_pnl.get(slot_id, 0.0) + pnl
                self._slot_trades[slot_id] = self._slot_trades.get(slot_id, 0) + 1
                if pnl > 0:
                    self._session_wins += 1

                win_rate = (self._session_wins / self._session_trades * 100) if self._session_trades > 0 else 0

                emoji = "✅" if pnl > 0 else "❌"
                log.info(
                    "%s [%s] PAPER EXIT: %s %s entry=$%.4f exit=$%.4f pnl=$%+.4f  "
                    "hold=%dt  type=%s  [session: %d trades, %.0f%% WR, $%+.2f]",
                    emoji, slot_id, signal.side, signal.reason,
                    last_trade["entry_price"], last_trade["exit_price"], pnl,
                    last_trade["hold_ticks"], last_trade["exit_type"],
                    self._session_trades, win_rate, self._session_pnl,
                )

                self._write_trade({
                    "ts": now_iso,
                    "slot_id": slot_id,
                    **last_trade,
                    "session_pnl": round(self._session_pnl, 4),
                    "session_trades": self._session_trades,
                    "session_win_rate": round(win_rate, 1),
                })

        elif signal.action == "HOLD_TO_EXPIRY":
            log.info(
                "🏁 [%s] PAPER HOLD_TO_EXPIRY: %s — %s",
                slot_id, signal.side, signal.reason,
            )

        return signal

    def _write_signal(self, data: dict) -> None:
        if self._signal_file is not None:
            self._signal_file.write(json.dumps(data, default=str) + "\n")
            self._signal_file.flush()

    def _write_trade(self, data: dict) -> None:
        if self._trade_file is not None:
            self._trade_file.write(json.dumps(data, default=str) + "\n")
            self._trade_file.flush()

    def print_daily_summary(self) -> None:
        """Print daily summary to log."""
        if self._session_trades == 0:
            log.info("📊 PAPER DAILY: no trades yet")
            return

        wr = (self._session_wins / self._session_trades * 100) if self._session_trades > 0 else 0

        # Per-slot breakdown
        slot_parts = []
        for sid in sorted(self._slot_trades.keys()):
            cnt = self._slot_trades[sid]
            if cnt > 0:
                slot_parts.append(f"{sid}={cnt}/${self._slot_pnl[sid]:+.2f}")

        log.info(
            "📊 PAPER DAILY: entries=%d exits=%d pnl_today=$%+.2f | "
            "SESSION: %d trades, %.0f%% WR, $%+.2f total | %s",
            self._daily_entries, self._daily_exits, self._daily_pnl,
            self._session_trades, wr, self._session_pnl,
            "  ".join(slot_parts) if slot_parts else "no slot data",
        )

    def close(self) -> None:
        """Clean up file handles."""
        if self._trade_file is not None:
            self._trade_file.close()
        if self._signal_file is not None:
            self._signal_file.close()


async def paper_trader_worker(
    cfg: Config,
    state: SharedState,
    tick_queue: asyncio.Queue[dict],
) -> None:
    """Async worker that reads ticks from shared state and paper trades all slots."""
    strat_cfg = StrategyConfig()
    slot_ids = [slot.slot_id for slot in cfg.market_slots]
    trader = PaperTrader(strat_cfg, slot_ids)

    log.info(
        "Paper trader starting: %d slots  zscore=%.1f  btc_pm_rate=%.4f  "
        "mid_range=[%.2f, %.2f]  min_bn_move=%.1f",
        len(slot_ids),
        strat_cfg.zscore_entry_threshold,
        strat_cfg.btc_to_pm_base_rate,
        strat_cfg.min_mid_entry,
        strat_cfg.max_mid_entry,
        strat_cfg.min_bn_move_abs,
    )

    from latpoly.workers.signal import (
        build_normalized_tick, SlotSignalState,
    )

    # Per-slot signal state (independent from signal worker)
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

    log.info("Paper trader ready — running on live data (%d slots)", len(slot_ids))

    consecutive_errors = 0
    max_consecutive_errors = 50

    try:
        while not state.shutdown.is_set():
            t0 = asyncio.get_running_loop().time()

            try:
                for slot_def in cfg.market_slots:
                    sid = slot_def.slot_id
                    bn = state.get_binance(slot_def.binance_symbol)
                    pm = state.get_polymarket(sid)

                    # Skip slots that aren't ready
                    if bn.best_bid is None or pm.yes_best_bid is None:
                        continue

                    ss = slot_signal_states[sid]

                    # Market rotation
                    cid = pm.market.condition_id
                    if cid != ss.last_condition_id:
                        ss.clear()
                        ss.last_condition_id = cid
                        log.info("Paper trader [%s]: market rotated", sid)

                    # Build tick
                    tick = build_normalized_tick(
                        bn, pm,
                        ss.price_history, ss.poly_tracker,
                        ss.zscore_bn_move, ss.zscore_ret1s,
                    )
                    tick["slot_id"] = sid

                    # Feed to strategy
                    trader.on_tick(tick)

                consecutive_errors = 0  # reset on success

            except asyncio.CancelledError:
                raise  # re-raise cancellation
            except Exception:
                consecutive_errors += 1
                if consecutive_errors <= 5 or consecutive_errors % 100 == 0:
                    log.exception("Paper trader tick error (#%d)", consecutive_errors)
                if consecutive_errors >= max_consecutive_errors:
                    log.error("Paper trader: too many consecutive errors (%d), stopping",
                              consecutive_errors)
                    break

            # Periodic summary
            now = time.time()
            if now - last_summary >= summary_interval:
                trader.print_daily_summary()
                last_summary = now

            elapsed = asyncio.get_running_loop().time() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    except asyncio.CancelledError:
        log.info("Paper trader cancelled")
    finally:
        trader.print_daily_summary()
        trader.close()
        log.info("Paper trader stopped. Session: %d trades, $%+.2f",
                 trader._session_trades, trader._session_pnl)
