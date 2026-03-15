"""Entry point — orchestrates 4 workers + health monitor."""

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
    from latpoly.workers.polymarket_ws import polymarket_worker
    from latpoly.workers.signal import signal_worker
    from latpoly.workers.writer import writer_worker

    tasks = [
        asyncio.create_task(binance_worker(cfg, state), name="W1-binance"),
        asyncio.create_task(polymarket_worker(cfg, state), name="W2-polymarket"),
        asyncio.create_task(signal_worker(cfg, state, writer_queue), name="W3-signal"),
        asyncio.create_task(writer_worker(cfg, state, writer_queue), name="W4-writer"),
        asyncio.create_task(health_loop(cfg, state, writer_queue), name="health"),
    ]

    # Shutdown handler
    loop = asyncio.get_running_loop()

    def _shutdown_signal() -> None:
        log.info("Shutdown signal received")
        state.shutdown.set()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown_signal)
    # On Windows, KeyboardInterrupt will propagate naturally

    # Wait for shutdown event
    await state.shutdown.wait()
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
    log.info("Config: symbol=%s signal_interval=%.2fs", cfg.symbol, cfg.signal_interval)
    log.info("Binance WS: %s", cfg.binance_ws_url)
    log.info("Polymarket WS: %s", cfg.poly_ws_url)

    try:
        asyncio.run(_run(cfg))
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    entry()
