"""W4 — Batched JSONL writer with per-session and daily file rotation."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import orjson

from latpoly.config import Config
from latpoly.shared_state import SharedState

log = logging.getLogger(__name__)


class _FileHandle:
    """Manages a single JSONL output file with lazy open."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: Optional[object] = None

    def _ensure_open(self) -> object:
        if self._fh is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self._path, "ab")  # noqa: SIM115
            log.info("Opened file: %s", self._path)
        return self._fh

    def write_batch(self, lines: list[bytes]) -> int:
        fh = self._ensure_open()
        data = b"".join(lines)
        fh.write(data)  # type: ignore[union-attr]
        fh.flush()  # type: ignore[union-attr]
        return len(data)

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()  # type: ignore[union-attr]
            self._fh = None


def _session_path(data_dir: str, condition_id: str, start_ts: float) -> Path:
    dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    ts_str = dt.strftime("%H%M%S")
    cid_short = condition_id[:12] if condition_id else "unknown"
    return Path(data_dir) / "sessions" / f"{date_str}_{cid_short}_{ts_str}.jsonl"


def _daily_path(data_dir: str) -> Path:
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    return Path(data_dir) / "daily" / f"{date_str}_merged.jsonl"


async def writer_worker(
    cfg: Config,
    state: SharedState,
    queue: asyncio.Queue[dict],
) -> None:
    """Consume normalized ticks from queue and write to JSONL files."""
    log.info("Writer worker starting")

    session_fh: Optional[_FileHandle] = None
    daily_fh: Optional[_FileHandle] = None
    current_condition_id: str = ""
    current_date: str = ""
    session_start_ts: float = 0.0
    batch: list[bytes] = []
    batch_ticks: list[dict] = []  # keep tick dicts for metadata inspection
    last_flush = time.monotonic()

    try:
        while not state.shutdown.is_set():
            # Drain queue with timeout for periodic flush
            try:
                tick = await asyncio.wait_for(queue.get(), timeout=cfg.writer_batch_timeout)
                line = orjson.dumps(tick) + b"\n"
                batch.append(line)
                batch_ticks.append(tick)
            except asyncio.TimeoutError:
                pass  # just flush what we have
            except asyncio.CancelledError:
                break

            # Check if we need to flush
            now = time.monotonic()
            should_flush = (
                len(batch) >= cfg.writer_batch_size
                or (now - last_flush) >= cfg.writer_batch_timeout
            )

            if not batch or not should_flush:
                continue

            # Handle session file rotation (use latest tick in batch)
            cid = batch_ticks[-1].get("condition_id", "") if batch_ticks else ""
            if cid and cid != current_condition_id:
                if session_fh:
                    session_fh.close()
                current_condition_id = cid
                session_start_ts = time.time()
                session_fh = _FileHandle(
                    _session_path(cfg.data_dir, cid, session_start_ts)
                )
                log.info("New session file for market %s", cid[:12])

            # Handle daily file rotation
            today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            if today != current_date:
                if daily_fh:
                    daily_fh.close()
                current_date = today
                daily_fh = _FileHandle(_daily_path(cfg.data_dir))

            # Write batch (in thread to avoid blocking)
            lines = batch
            batch = []
            batch_ticks = []
            last_flush = now

            bytes_written = 0
            if session_fh:
                bytes_written += await asyncio.to_thread(session_fh.write_batch, lines)
            if daily_fh:
                bytes_written += await asyncio.to_thread(daily_fh.write_batch, lines)

            state.writer_records_written += len(lines)
            state.writer_bytes_written += bytes_written

    except asyncio.CancelledError:
        pass
    finally:
        # Flush remaining batch
        if batch:
            if session_fh:
                session_fh.write_batch(batch)
            if daily_fh:
                daily_fh.write_batch(batch)
            state.writer_records_written += len(batch)
            log.info("Flushed %d remaining records", len(batch))

        if session_fh:
            session_fh.close()
        if daily_fh:
            daily_fh.close()

        log.info(
            "Writer stopped. Total: %d records, %d bytes",
            state.writer_records_written,
            state.writer_bytes_written,
        )
