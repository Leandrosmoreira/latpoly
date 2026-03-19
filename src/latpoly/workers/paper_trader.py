"""W5 — Paper trading worker: runs StrategyEngine on live ticks (no real orders).

Consumes the same normalized ticks as the JSONL writer, feeds them to the
StrategyEngine, and logs every signal + simulated P&L to stderr and a
separate paper_trades.jsonl file.

This is Phase 3.1 — validates the strategy in real-time before going live.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from latpoly.config import Config
from latpoly.shared_state import SharedState
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import StrategyEngine, Signal

log = logging.getLogger(__name__)


class PaperTrader:
    """Wraps StrategyEngine with logging, file output, and daily stats."""

    def __init__(self, strat_cfg: StrategyConfig, output_dir: str = "data/paper") -> None:
        self.engine = StrategyEngine(strat_cfg)
        self.strat_cfg = strat_cfg
        self._tick_idx: int = 0
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._trade_file: Optional[object] = None
        self._signal_file: Optional[object] = None
        self._current_date: str = ""

        # Daily stats
        self._daily_signals: int = 0
        self._daily_entries: int = 0
        self._daily_exits: int = 0
        self._daily_pnl: float = 0.0
        self._session_pnl: float = 0.0
        self._session_trades: int = 0
        self._session_wins: int = 0

    def _rotate_files(self) -> None:
        """Open new output files if the date changed."""
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today == self._current_date:
            return

        # Close old files
        if self._trade_file is not None:
            self._trade_file.close()
        if self._signal_file is not None:
            self._signal_file.close()

        self._current_date = today
        trade_path = self._output_dir / f"paper_trades_{today}.jsonl"
        signal_path = self._output_dir / f"paper_signals_{today}.jsonl"

        self._trade_file = open(trade_path, "a")
        self._signal_file = open(signal_path, "a")
        log.info("Paper trader: writing to %s", trade_path)

        # Reset daily stats
        self._daily_signals = 0
        self._daily_entries = 0
        self._daily_exits = 0
        self._daily_pnl = 0.0

    def on_tick(self, tick: dict) -> Signal:
        """Process one live tick through the strategy engine."""
        self._rotate_files()
        self._tick_idx += 1

        signal = self.engine.on_tick(tick, self._tick_idx)

        if signal.action == "NONE":
            return signal

        self._daily_signals += 1
        now_iso = time.strftime("%H:%M:%S", time.gmtime())

        if signal.action in ("BUY_YES", "BUY_NO"):
            self._daily_entries += 1
            log.info(
                "📝 PAPER ENTRY: %s %s sz=%d @ $%.4f → target=$%.4f  edge=%.4f  [%s]",
                signal.action, signal.side, signal.size,
                signal.entry_price, signal.exit_target,
                signal.net_edge, signal.reason,
            )
            # Log signal to file
            self._write_signal({
                "ts": now_iso,
                "ts_ns": tick.get("ts_ns"),
                "action": signal.action,
                "side": signal.side,
                "entry_price": signal.entry_price,
                "exit_target": signal.exit_target,
                "size": signal.size,
                "net_edge": round(signal.net_edge, 6),
                "time_weight": round(signal.time_weight, 4),
                "reason": signal.reason,
                "tick_idx": self._tick_idx,
                "condition_id": tick.get("condition_id"),
                "mid_binance": tick.get("mid_binance"),
                "distance_to_strike": tick.get("distance_to_strike"),
                "tte_s": (tick.get("time_to_expiry_ms") or 0) / 1000.0,
            })

        elif signal.action == "EXIT":
            self._daily_exits += 1

            # Get the last closed trade from engine
            trades = self.engine.closed_trades
            if trades:
                last_trade = trades[-1]
                pnl = last_trade["pnl_net"]
                self._daily_pnl += pnl
                self._session_pnl += pnl
                self._session_trades += 1
                if pnl > 0:
                    self._session_wins += 1

                win_rate = (self._session_wins / self._session_trades * 100) if self._session_trades > 0 else 0

                emoji = "✅" if pnl > 0 else "❌"
                log.info(
                    "%s PAPER EXIT: %s %s entry=$%.4f exit=$%.4f pnl=$%+.4f  "
                    "hold=%dt  type=%s  [session: %d trades, %.0f%% WR, $%+.2f]",
                    emoji, signal.side, signal.reason,
                    last_trade["entry_price"], last_trade["exit_price"], pnl,
                    last_trade["hold_ticks"], last_trade["exit_type"],
                    self._session_trades, win_rate, self._session_pnl,
                )

                # Log trade to file
                self._write_trade({
                    "ts": now_iso,
                    **last_trade,
                    "session_pnl": round(self._session_pnl, 4),
                    "session_trades": self._session_trades,
                    "session_win_rate": round(win_rate, 1),
                })

        elif signal.action == "HOLD_TO_EXPIRY":
            log.info(
                "🏁 PAPER HOLD_TO_EXPIRY: %s — %s",
                signal.side, signal.reason,
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
            log.info("📊 PAPER DAILY: no trades today")
            return

        wr = (self._session_wins / self._session_trades * 100) if self._session_trades > 0 else 0
        log.info(
            "📊 PAPER DAILY: entries=%d exits=%d pnl_today=$%+.2f | "
            "SESSION: %d trades, %.0f%% WR, $%+.2f total",
            self._daily_entries, self._daily_exits, self._daily_pnl,
            self._session_trades, wr, self._session_pnl,
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
    """Async worker that reads ticks from the signal queue and paper trades.

    Runs alongside the existing workers. Does NOT modify shared state.
    """
    strat_cfg = StrategyConfig()
    trader = PaperTrader(strat_cfg)

    log.info(
        "Paper trader starting: zscore=%.1f  btc_pm_rate=%.4f  "
        "mid_range=[%.2f, %.2f]  min_bn_move=%.1f",
        strat_cfg.zscore_entry_threshold,
        strat_cfg.btc_to_pm_base_rate,
        strat_cfg.min_mid_entry,
        strat_cfg.max_mid_entry,
        strat_cfg.min_bn_move_abs,
    )

    # We need our own copy of ticks — subscribe via a separate mechanism.
    # Since signal_worker puts ticks on the writer queue, we'll build ticks
    # directly from shared state on the same interval.
    from latpoly.workers.signal import (
        build_normalized_tick, PriceHistory, PolyChangeTracker,
        EmaZScore,
    )

    price_history = PriceHistory()
    poly_tracker = PolyChangeTracker()
    zscore_bn_move = EmaZScore()
    zscore_ret1s = EmaZScore()

    interval = cfg.signal_interval
    last_condition_id = ""
    summary_interval = 300  # print summary every 5 min
    last_summary = 0.0

    # Wait for readiness
    while not state.shutdown.is_set() and not state.ready:
        await asyncio.sleep(0.5)

    if state.shutdown.is_set():
        trader.close()
        return

    log.info("Paper trader ready — running on live data")

    try:
        while not state.shutdown.is_set():
            t0 = asyncio.get_event_loop().time()

            # Market rotation
            cid = state.polymarket.market.condition_id
            if cid != last_condition_id:
                price_history.clear()
                poly_tracker.clear()
                zscore_bn_move.clear()
                zscore_ret1s.clear()
                last_condition_id = cid
                log.info("Paper trader: market rotated")

            # Build tick from shared state (same as signal worker)
            tick = build_normalized_tick(
                state, price_history, poly_tracker,
                zscore_bn_move, zscore_ret1s,
            )

            # Feed to strategy
            trader.on_tick(tick)

            # Periodic summary
            now = time.time()
            if now - last_summary >= summary_interval:
                trader.print_daily_summary()
                last_summary = now

            elapsed = asyncio.get_event_loop().time() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    except asyncio.CancelledError:
        log.info("Paper trader cancelled")
    finally:
        trader.print_daily_summary()
        trader.close()
        log.info("Paper trader stopped. Session: %d trades, $%+.2f",
                 trader._session_trades, trader._session_pnl)
