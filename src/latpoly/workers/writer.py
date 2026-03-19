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


def _session_path(data_dir: str, condition_id: str, start_ts: float, slot_id: str = "") -> Path:
    dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    ts_str = dt.strftime("%H%M%S")
    cid_short = condition_id[:12] if condition_id else "unknown"
    slot_prefix = f"{slot_id}_" if slot_id else ""
    return Path(data_dir) / "sessions" / f"{date_str}_{slot_prefix}{cid_short}_{ts_str}.jsonl"


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

    # Per-slot session files (keyed by slot_id)
    session_fhs: dict[str, _FileHandle] = {}
    session_cids: dict[str, str] = {}  # slot_id -> last condition_id
    daily_fh: Optional[_FileHandle] = None
    current_date: str = ""
    batch: list[bytes] = []
    batch_ticks: list[dict] = []
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
                pass
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

            # Handle daily file rotation
            today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            if today != current_date:
                if daily_fh:
                    daily_fh.close()
                current_date = today
                daily_fh = _FileHandle(_daily_path(cfg.data_dir))

            # Group batch by slot for session file routing
            # But write ALL to daily file
            lines = batch
            ticks = batch_ticks
            batch = []
            batch_ticks = []
            last_flush = now

            # Handle per-slot session file rotation
            for tick_data in ticks:
                cid = tick_data.get("condition_id", "")
                slot_id = tick_data.get("slot_id", "")
                key = slot_id or "_default"

                if cid and cid != session_cids.get(key, ""):
                    # Close old session file for this slot
                    old_fh = session_fhs.get(key)
                    if old_fh:
                        old_fh.close()
                    session_cids[key] = cid
                    session_fhs[key] = _FileHandle(
                        _session_path(cfg.data_dir, cid, time.time(), slot_id)
                    )
                    log.info("New session file for %s market %s", slot_id or "default", cid[:12])

            # Write batch to daily file
            bytes_written = 0
            if daily_fh:
                bytes_written += await asyncio.to_thread(daily_fh.write_batch, lines)

            # Write each line to its slot's session file
            for line_bytes, tick_data in zip(lines, ticks):
                slot_id = tick_data.get("slot_id", "")
                key = slot_id or "_default"
                sfh = session_fhs.get(key)
                if sfh:
                    bytes_written += sfh.write_batch([line_bytes])

            state.writer_records_written += len(lines)
            state.writer_bytes_written += bytes_written

    except asyncio.CancelledError:
        pass
    finally:
        # Flush remaining batch
        if batch:
            if daily_fh:
                daily_fh.write_batch(batch)
            # Write to session files too
            for line_bytes, tick_data in zip(batch, batch_ticks):
                slot_id = tick_data.get("slot_id", "")
                key = slot_id or "_default"
                sfh = session_fhs.get(key)
                if sfh:
                    sfh.write_batch([line_bytes])
            state.writer_records_written += len(batch)
            log.info("Flushed %d remaining records", len(batch))

        for sfh in session_fhs.values():
            sfh.close()
        if daily_fh:
            daily_fh.close()

        log.info(
            "Writer stopped. Total: %d records, %d bytes",
            state.writer_records_written,
            state.writer_bytes_written,
        )
