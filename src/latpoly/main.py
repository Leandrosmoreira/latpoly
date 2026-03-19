"""Entry point — orchestrates N workers + health monitor."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from latpoly.config import Config
from latpoly.loop_setup import configure_loop
from latpoly.shared_state import SharedState

log = logging.getLogger("latpoly")

SHUTDOWN_TIMEOUT = 5.0


async def _run(cfg: Config) -> None:
    state = SharedState()
    writer_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=cfg.queue_maxsize)

    # Import workers
    from latpoly.health import health_loop
    from latpoly.workers.binance_ws import binance_worker
    from latpoly.workers.polymarket_ws import polymarket_slot_worker
    from latpoly.workers.signal import signal_worker
    from latpoly.workers.writer import writer_worker
    from latpoly.workers.paper_trader import paper_trader_worker

    tasks = []

    # Binance workers: one per unique symbol
    for sym in sorted(cfg.binance_symbols):
        tasks.append(
            asyncio.create_task(binance_worker(cfg, state, sym), name=f"W1-{sym}")
        )

    # Polymarket workers: one per slot
    for i, slot in enumerate(cfg.market_slots):
        tasks.append(
            asyncio.create_task(
                polymarket_slot_worker(cfg, state, slot),
                name=f"W2-{slot.slot_id}",
            )
        )

    # Shared workers
    tasks.append(asyncio.create_task(signal_worker(cfg, state, writer_queue), name="W3-signal"))
    tasks.append(asyncio.create_task(writer_worker(cfg, state, writer_queue), name="W4-writer"))
    tasks.append(asyncio.create_task(paper_trader_worker(cfg, state, writer_queue), name="W5-paper"))
    tasks.append(asyncio.create_task(health_loop(cfg, state, writer_queue), name="health"))

    # Shutdown handler
    loop = asyncio.get_running_loop()

    def _shutdown_signal() -> None:
        log.info("Shutdown signal received")
        state.shutdown.set()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown_signal)

    # Monitor tasks for crashes while waiting for shutdown
    while not state.shutdown.is_set():
        await asyncio.sleep(5.0)
        for t in tasks:
            if t.done() and not t.cancelled():
                exc = t.exception()
                if exc is not None:
                    log.error("Worker %s crashed: %s: %s", t.get_name(),
                              type(exc).__name__, exc)

    log.info("Shutting down workers...")

    # Cancel all tasks
    for t in tasks:
        t.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for t, r in zip(tasks, results):
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            log.error("Worker %s failed: %s", t.get_name(), r)

    log.info("All workers stopped")


def entry() -> None:
    configure_loop()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    cfg = Config()
    slots = cfg.market_slots
    log.info(
        "Config: %d market slots, %d binance symbols, signal_interval=%.2fs",
        len(slots), len(cfg.binance_symbols), cfg.signal_interval,
    )
    for slot in slots:
        log.info("  Slot: %s (%s / %s / %ss)", slot.slot_id, slot.binance_symbol,
                 slot.coin, slot.timeframe)

    try:
        asyncio.run(_run(cfg))
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    entry()
