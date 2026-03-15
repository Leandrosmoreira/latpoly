"""Health monitor — periodic metrics logging (runs as coroutine in main, not a 5th worker)."""

from __future__ import annotations

import asyncio
import logging
import time

import orjson

from latpoly.config import Config
from latpoly.shared_state import SharedState

log = logging.getLogger(__name__)


def _compute_metrics(state: SharedState, queue: asyncio.Queue, uptime_start: float) -> dict:
    """Build health metrics dict from current state."""
    now_ns = time.time_ns()
    now_mono = time.monotonic()
    bn = state.binance
    pm = state.polymarket
    mkt = pm.market

    age_bn = (now_ns - bn.ts_local_recv_ns) / 1e9 if bn.ts_local_recv_ns else -1.0
    age_pm = (now_ns - pm.ts_local_recv_ns) / 1e9 if pm.ts_local_recv_ns else -1.0

    time_remaining = max(0.0, mkt.end_ts_s - time.time()) if mkt.end_ts_s else -1.0

    return {
        "ts": time.time(),
        "uptime_s": round(now_mono - uptime_start, 1),
        "binance_events": bn.event_count,
        "binance_reconnects": bn.reconnect_count,
        "binance_age_s": round(age_bn, 2),
        "binance_mid": bn.mid,
        "polymarket_events": pm.event_count,
        "polymarket_reconnects": pm.reconnect_count,
        "polymarket_age_s": round(age_pm, 2),
        "pm_mid_yes": pm.mid_yes,
        "pm_mid_no": pm.mid_no,
        "signal_ticks": state.signal_tick_count,
        "writer_records": state.writer_records_written,
        "writer_bytes": state.writer_bytes_written,
        "queue_depth": queue.qsize(),
        "market_condition_id": mkt.condition_id[:12] if mkt.condition_id else "",
        "market_strike": mkt.strike,
        "market_time_remaining_s": round(time_remaining, 1),
        "ready": state.ready,
    }


async def health_loop(
    cfg: Config,
    state: SharedState,
    queue: asyncio.Queue,
) -> None:
    """Log health metrics periodically to stderr."""
    interval = cfg.health_interval
    uptime_start = time.monotonic()
    log.info("Health monitor starting (interval=%.0fs)", interval)

    try:
        while not state.shutdown.is_set():
            try:
                await asyncio.wait_for(state.shutdown.wait(), timeout=interval)
                break  # shutdown
            except asyncio.TimeoutError:
                pass

            metrics = _compute_metrics(state, queue, uptime_start)

            # Staleness alerts
            if metrics["binance_age_s"] > 5.0 and metrics["binance_age_s"] != -1.0:
                log.warning("STALE Binance data: %.1fs", metrics["binance_age_s"])
            if metrics["polymarket_age_s"] > 30.0 and metrics["polymarket_age_s"] != -1.0:
                log.warning("STALE Polymarket data: %.1fs", metrics["polymarket_age_s"])

            # Log compact health line
            log.info(
                "HEALTH up=%.0fs bn=%d/%.1fs pm=%d/%.1fs ticks=%d q=%d mkt=%s ttx=%.0fs",
                metrics["uptime_s"],
                metrics["binance_events"],
                metrics["binance_age_s"],
                metrics["polymarket_events"],
                metrics["polymarket_age_s"],
                metrics["signal_ticks"],
                metrics["queue_depth"],
                metrics["market_condition_id"],
                metrics["market_time_remaining_s"],
            )

    except asyncio.CancelledError:
        log.info("Health monitor stopped")
