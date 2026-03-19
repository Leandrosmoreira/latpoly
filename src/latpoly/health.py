"""Health monitor — periodic metrics logging (multi-market aware)."""

from __future__ import annotations

import asyncio
import logging
import time

from latpoly.config import Config
from latpoly.shared_state import SharedState

log = logging.getLogger(__name__)


def _compute_metrics(cfg: Config, state: SharedState, queue: asyncio.Queue, uptime_start: float) -> dict:
    """Build health metrics dict from current state."""
    now_ns = time.time_ns()
    now_mono = time.monotonic()
    now_s = time.time()

    # Per-slot readiness
    slots_ready = 0
    slots_total = len(cfg.market_slots)
    slot_details = []

    for slot_def in cfg.market_slots:
        bn = state.binance_states.get(slot_def.binance_symbol)
        pm = state.polymarket_states.get(slot_def.slot_id)
        bn_ok = bn is not None and bn.best_bid is not None
        pm_ok = pm is not None and pm.yes_best_bid is not None
        ready = bn_ok and pm_ok
        if ready:
            slots_ready += 1

        # Track staleness per slot
        bn_age = -1.0
        pm_age = -1.0
        ttx = -1.0
        if bn is not None and bn.ts_local_recv_ns:
            bn_age = (now_ns - bn.ts_local_recv_ns) / 1e9
        if pm is not None and pm.ts_local_recv_ns:
            pm_age = (now_ns - pm.ts_local_recv_ns) / 1e9
        if pm is not None and pm.market.end_ts_s:
            ttx = max(0.0, pm.market.end_ts_s - now_s)

        slot_details.append({
            "slot_id": slot_def.slot_id,
            "ready": ready,
            "bn_age_s": round(bn_age, 1),
            "pm_age_s": round(pm_age, 1),
            "ttx_s": round(ttx, 0),
            "pm_events": pm.event_count if pm else 0,
        })

    # Aggregate Binance stats
    total_bn_events = sum(bn.event_count for bn in state.binance_states.values())
    total_pm_events = sum(pm.event_count for pm in state.polymarket_states.values())

    return {
        "ts": now_s,
        "uptime_s": round(now_mono - uptime_start, 1),
        "slots_ready": slots_ready,
        "slots_total": slots_total,
        "bn_events": total_bn_events,
        "pm_events": total_pm_events,
        "signal_ticks": state.signal_tick_count,
        "writer_records": state.writer_records_written,
        "queue_depth": queue.qsize(),
        "slot_details": slot_details,
    }


async def health_loop(
    cfg: Config,
    state: SharedState,
    queue: asyncio.Queue,
) -> None:
    """Log health metrics periodically to stderr."""
    interval = cfg.health_interval
    uptime_start = time.monotonic()
    log.info("Health monitor starting (interval=%.0fs, %d slots)",
             interval, len(cfg.market_slots))

    try:
        while not state.shutdown.is_set():
            try:
                await asyncio.wait_for(state.shutdown.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

            metrics = _compute_metrics(cfg, state, queue, uptime_start)

            # Staleness alerts per-slot
            for sd in metrics["slot_details"]:
                if sd["bn_age_s"] > 5.0 and sd["bn_age_s"] != -1.0:
                    log.warning("STALE [%s] Binance: %.1fs", sd["slot_id"], sd["bn_age_s"])
                if sd["pm_age_s"] > 30.0 and sd["pm_age_s"] != -1.0:
                    log.warning("STALE [%s] Polymarket: %.1fs", sd["slot_id"], sd["pm_age_s"])

            # Compact health line
            log.info(
                "HEALTH up=%.0fs slots=%d/%d bn=%d pm=%d ticks=%d q=%d wr=%d",
                metrics["uptime_s"],
                metrics["slots_ready"], metrics["slots_total"],
                metrics["bn_events"],
                metrics["pm_events"],
                metrics["signal_ticks"],
                metrics["queue_depth"],
                metrics["writer_records"],
            )

            # Per-slot one-liner (only for ready slots, compact)
            ready_slots = [sd for sd in metrics["slot_details"] if sd["ready"]]
            if ready_slots:
                parts = [
                    f"{sd['slot_id']}(ttx={sd['ttx_s']:.0f}s,ev={sd['pm_events']})"
                    for sd in ready_slots
                ]
                log.info("  SLOTS: %s", "  ".join(parts))

    except asyncio.CancelledError:
        log.info("Health monitor stopped")
