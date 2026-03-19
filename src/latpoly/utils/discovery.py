"""Polymarket market discovery via Gamma API — generalized for multi-market.

Supports two slug patterns:
  - timestamp: "btc-updown-15m-{window_ts}"
  - human_date: "bitcoin-up-or-down-{month}-{day}-{year}-{hour}{ampm}-et"

Backward-compatible: discover_btc_15m_market() still works.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

import aiohttp
import orjson

from latpoly.shared_state import MarketInfo

if TYPE_CHECKING:
    from latpoly.config import MarketSlotDef

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Window / slug helpers
# ---------------------------------------------------------------------------


def current_window_ts(server_time_s: float, window_seconds: int = 900) -> int:
    """Round down to the nearest window boundary (epoch seconds)."""
    return int(server_time_s // window_seconds) * window_seconds


def make_slug(slot_def: MarketSlotDef, window_ts: int) -> str:
    """Build the Gamma API slug for a market slot.

    timestamp pattern:   btc-updown-15m-1710504000
    human_date pattern:  bitcoin-up-or-down-march-18-2026-11pm-et
    """
    if slot_def.slug_pattern == "timestamp":
        return f"{slot_def.coin.lower()}-updown-{slot_def.timeframe}-{window_ts}"
    elif slot_def.slug_pattern == "human_date":
        return _make_human_date_slug(slot_def.coin, window_ts)
    else:
        raise ValueError(f"Unknown slug_pattern: {slot_def.slug_pattern}")


def _utc_to_et_offset(utc_dt: datetime) -> int:
    """Return ET offset in seconds (-18000 for EST, -14400 for EDT).

    US Eastern DST: second Sunday of March 2:00 AM to first Sunday of November 2:00 AM.
    """
    year = utc_dt.year
    # Second Sunday of March
    mar1 = datetime(year, 3, 1, tzinfo=timezone.utc)
    dst_start_day = 14 - mar1.weekday()  # weekday: Mon=0..Sun=6
    if dst_start_day < 8:
        dst_start_day += 7
    dst_start = datetime(year, 3, dst_start_day, 7, 0, tzinfo=timezone.utc)  # 2AM ET = 7AM UTC

    # First Sunday of November
    nov1 = datetime(year, 11, 1, tzinfo=timezone.utc)
    dst_end_day = 7 - nov1.weekday()
    if dst_end_day < 1:
        dst_end_day += 7
    dst_end = datetime(year, 11, dst_end_day, 6, 0, tzinfo=timezone.utc)  # 2AM EDT = 6AM UTC

    if dst_start <= utc_dt < dst_end:
        return -14400  # EDT (UTC-4)
    return -18000  # EST (UTC-5)


def _make_human_date_slug(coin: str, window_ts: int) -> str:
    """Build human-date slug like 'bitcoin-up-or-down-march-18-2026-11pm-et'.

    The window_ts is the START of the window (the market's end time).
    Polymarket uses ET timezone for the slug.
    """
    utc_dt = datetime.fromtimestamp(window_ts, tz=timezone.utc)
    et_offset = _utc_to_et_offset(utc_dt)
    et_dt = datetime.fromtimestamp(window_ts + et_offset, tz=timezone.utc)

    month_name = et_dt.strftime("%B").lower()   # "march"
    day = et_dt.day                              # 18
    year = et_dt.year                            # 2026
    hour_12 = et_dt.strftime("%I").lstrip("0")   # "11"
    ampm = et_dt.strftime("%p").lower()          # "pm"

    return f"{coin.lower()}-up-or-down-{month_name}-{day}-{year}-{hour_12}{ampm}-et"


# Backward-compat wrapper
def make_slug_legacy(coin: str, window_ts: int) -> str:
    """Legacy slug for btc-15m (backward-compat)."""
    return f"{coin.lower()}-updown-15m-{window_ts}"


# ---------------------------------------------------------------------------
# Strike extraction
# ---------------------------------------------------------------------------


def _extract_strike(text: str) -> float:
    """Extract a price strike from market question text.

    Examples:
    - "Will Bitcoin be above $85,000 at 14:30 UTC?" -> 85000.0
    - "Will ETH be above $2,400.50?" -> 2400.5
    - "SOL above 140?" -> 140.0
    """
    clean = text.replace(",", "")
    # $-prefixed is reliable — any value > 0
    for match in re.finditer(r"\$([\d]+(?:\.\d+)?)", clean):
        try:
            val = float(match.group(1))
            if val > 0:
                return val
        except ValueError:
            continue
    # Bare numbers: require > 50 to avoid dates (March 18), times (11, 30), etc.
    for match in re.finditer(r"(?<!\d)([\d]+(?:\.\d+)?)(?!\d)", clean):
        try:
            val = float(match.group(1))
            if val > 50:
                return val
        except ValueError:
            continue
    return 0.0


# ---------------------------------------------------------------------------
# Main discovery function (generalized)
# ---------------------------------------------------------------------------


async def discover_market(
    gamma_url: str,
    clob_url: str,
    slot_def: MarketSlotDef,
    max_retries: int = 3,
) -> Optional[MarketInfo]:
    """Find the current active market for any slot definition.

    Calculates the current window timestamp, builds the slug, and fetches
    from Gamma API. Tries current window first, then next window.
    """
    now = time.time()
    window_ts = current_window_ts(now, slot_def.window_seconds)
    gamma_base = gamma_url.rstrip("/")

    # Try current window, then next window (for transition periods)
    slugs_to_try = [
        make_slug(slot_def, window_ts),
        make_slug(slot_def, window_ts + slot_def.window_seconds),
    ]

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession() as session:
        for slug in slugs_to_try:
            url = f"{gamma_base}/events/slug/{slug}"
            last_err = None

            for attempt in range(1, max_retries + 1):
                try:
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status == 404:
                            log.debug("[%s] Slug not found: %s (attempt %d)",
                                      slot_def.slot_id, slug, attempt)
                            last_err = "not_found"
                            if attempt < max_retries:
                                await asyncio.sleep(min(0.5 * attempt, 3.0))
                                continue
                            break

                        resp.raise_for_status()
                        raw = await resp.read()
                        event = orjson.loads(raw)

                except Exception as exc:
                    log.warning("[%s] Gamma fetch error for %s: %s",
                                slot_def.slot_id, slug, exc)
                    last_err = str(exc)
                    if attempt < max_retries:
                        await asyncio.sleep(min(0.5 * attempt, 3.0))
                        continue
                    break
                else:
                    result = _parse_event(event, slug)
                    if result is not None:
                        return result
                    break

            log.debug("[%s] Slug %s exhausted (%s), trying next",
                      slot_def.slot_id, slug, last_err)

    log.warning("[%s] No market found for window_ts=%d", slot_def.slot_id, window_ts)
    return None


# Backward-compat wrapper
async def discover_btc_15m_market(
    gamma_url: str,
    clob_url: str,
    coin: str = "btc",
    max_retries: int = 3,
) -> Optional[MarketInfo]:
    """Legacy discovery for BTC 15m (backward-compat)."""
    from latpoly.config import MarketSlotDef
    slot = MarketSlotDef("btc-15m", "btcusdt", coin, "15m", 900, "timestamp")
    return await discover_market(gamma_url, clob_url, slot, max_retries)


# ---------------------------------------------------------------------------
# Event parser
# ---------------------------------------------------------------------------


def _parse_event(event: dict, slug: str) -> Optional[MarketInfo]:
    """Parse a Gamma API event response into MarketInfo."""
    markets = event.get("markets", [])
    if not markets:
        log.warning("No markets in event for %s", slug)
        return None

    market = markets[0]

    # Parse clobTokenIds
    raw_tokens = market.get("clobTokenIds", [])
    if isinstance(raw_tokens, str):
        try:
            clob_tokens = json.loads(raw_tokens)
        except json.JSONDecodeError:
            log.error("Cannot parse clobTokenIds for %s: %s", slug, raw_tokens[:100])
            return None
    else:
        clob_tokens = raw_tokens

    if len(clob_tokens) < 2:
        log.error("Missing clobTokenIds for %s: %s", slug, clob_tokens)
        return None

    # Parse end date
    end_str = market.get("endDate") or event.get("endDate", "")
    end_ts_s = 0.0
    if end_str:
        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            end_ts_s = end_dt.timestamp()
        except Exception:
            log.warning("Cannot parse endDate for %s: %s", slug, end_str)

    # Extract strike
    question = market.get("question", "") or event.get("title", "")
    strike = _extract_strike(question)

    condition_id = market.get("conditionId", "") or market.get("condition_id", "")

    info = MarketInfo(
        condition_id=condition_id,
        slug=slug,
        yes_token_id=clob_tokens[0],
        no_token_id=clob_tokens[1],
        end_ts_s=end_ts_s,
        strike=strike,
        question=question,
    )

    log.info(
        "Discovered market: %s | strike=%.1f | ends=%s | yes=%s no=%s",
        info.question[:60] if info.question else slug,
        info.strike,
        time.strftime("%H:%M:%S", time.gmtime(info.end_ts_s)) if info.end_ts_s else "?",
        info.yes_token_id[:16],
        info.no_token_id[:16],
    )

    return info


# ---------------------------------------------------------------------------
# Book snapshot (unchanged)
# ---------------------------------------------------------------------------


async def fetch_book_snapshot(
    clob_url: str,
    token_id: str,
) -> tuple[Optional[float], Optional[float], list, list]:
    """Fetch current best bid/ask and full book depth from CLOB REST API.

    Returns (best_bid, best_ask, bids_levels, asks_levels).
    """
    async with aiohttp.ClientSession() as session:
        try:
            url = f"{clob_url}/book"
            params = {"token_id": token_id}
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    log.warning("CLOB book API returned %d for %s", resp.status, token_id[:12])
                    return None, None, [], []
                raw = await resp.read()
                book = orjson.loads(raw)
        except Exception:
            log.exception("Failed to fetch book for %s", token_id[:12])
            return None, None, [], []

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    best_bid = float(bids[-1]["price"]) if bids else None
    best_ask = float(asks[-1]["price"]) if asks else None

    bids_levels = []
    for entry in reversed(bids):
        try:
            bids_levels.append((float(entry["price"]), float(entry["size"])))
        except (ValueError, TypeError, KeyError):
            continue
    asks_levels = []
    for entry in reversed(asks):
        try:
            asks_levels.append((float(entry["price"]), float(entry["size"])))
        except (ValueError, TypeError, KeyError):
            continue

    return best_bid, best_ask, bids_levels[:10], asks_levels[:10]
