"""Platform-aware event loop configuration."""

from __future__ import annotations

import asyncio
import logging
import sys

log = logging.getLogger(__name__)


def configure_loop() -> None:
    """Install uvloop on Linux, fall back to default on Windows."""
    if sys.platform == "linux":
        try:
            import uvloop  # type: ignore[import-untyped]

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            log.info("Event loop: uvloop")
            return
        except ImportError:
            log.warning("uvloop not installed, using default loop")
    else:
        log.info("Event loop: default (%s)", sys.platform)
