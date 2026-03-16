"""Polymarket market discovery via Gamma API — slug-based approach.

Instead of searching 100+ events by keywords (unreliable), we build the
deterministic slug for the current 15-minute window and fetch it directly:

    GET /events/slug/btc-updown-15m-{window_ts}

This matches the proven approach used in the production recorder.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import orjson

from latpoly.shared_state import MarketInfo

log = logging.getLogger(__name__)

WINDOW_SECONDS_15M = 900  # 15 minutes


# ---------------------------------------------------------------------------
# Window / slug helpers
# ---------------------------------------------------------------------------


def current_window_ts(server_time_s: float) -> int:
    """Round down to the nearest 15-minute boundary (epoch seconds)."""
    return int(server_time_s // WINDOW_SECONDS_15M) * WINDOW_SECONDS_15M


def make_slug(coin: str, window_ts: int) -> str:
    """Build the Gamma API slug for a 15m updown market.

    Example: btc-updown-15m-1710504000
    """
    return f"{coin.lower()}-updown-15m-{window_ts}"


# ---------------------------------------------------------------------------
# Strike extraction
# ---------------------------------------------------------------------------


def _extract_strike(text: str) -> float:
    """Extract a BTC price strike from market question text.

    Examples:
    - "Will Bitcoin be above $85,000 at 14:30 UTC?" -> 85000.0
    - "BTC above 84500.50?" -> 84500.5
    """
    # Remove commas and look for dollar amounts
    clean = text.replace(",", "")
    # Try $-prefixed first (most reliable), then all numbers
    for pattern in [r"\$([\d]+(?:\.\d+)?)", r"([\d]+(?:\.\d+)?)"]:
        for match in re.finditer(pattern, clean):
            try:
                val = float(match.group(1))
                if 10_000 < val < 500_000:
                    return val
            except ValueError:
                continue
    return 0.0


# ---------------------------------------------------------------------------
# Main discovery function
# ---------------------------------------------------------------------------


async def discover_btc_15m_market(
    gamma_url: str,
    clob_url: str,
    coin: str = "btc",
    max_retries: int = 3,
) -> Optional[MarketInfo]:
    """Find the current active BTC 15-minute prediction market.

    Uses the deterministic slug approach: calculates the current 15m window
    timestamp, builds the slug, and fetches directly from Gamma API.

    If the current window slug returns 404, also tries the next window
    (useful near window transitions).
    """
    now = time.time()
    window_ts = current_window_ts(now)
    gamma_base = gamma_url.rstrip("/")

    # Try current window first, then next window (for transition periods)
    slugs_to_try = [
        make_slug(coin, window_ts),
        make_slug(coin, window_ts + WINDOW_SECONDS_15M),
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
                            log.debug("Slug not found: %s (attempt %d)", slug, attempt)
                            last_err = "not_found"
                            if attempt < max_retries:
                                await asyncio.sleep(min(0.5 * attempt, 3.0))
                                continue
                            break  # try next slug

                        resp.raise_for_status()
                        raw = await resp.read()
                        event = orjson.loads(raw)

                except Exception as exc:
                    log.warning("Gamma fetch error for %s: %s", slug, exc)
                    last_err = str(exc)
                    if attempt < max_retries:
                        await asyncio.sleep(min(0.5 * attempt, 3.0))
                        continue
                    break  # try next slug
                else:
                    # Successfully got the event — parse it
                    result = _parse_event(event, slug)
                    if result is not None:
                        return result
                    break  # event parsed but no valid market

            log.debug("Slug %s exhausted (%s), trying next", slug, last_err)

    log.warning("No BTC 15m market found for window_ts=%d", window_ts)
    return None


# ---------------------------------------------------------------------------
# Event parser
# ---------------------------------------------------------------------------


def _parse_event(event: dict, slug: str) -> Optional[MarketInfo]:
    """Parse a Gamma API event response into MarketInfo."""
    markets = event.get("markets", [])
    if not markets:
        log.warning("No markets in event for %s", slug)
        return None

    # Use first market (15m events have a single market)
    market = markets[0]

    # Parse clobTokenIds — can be JSON string or list
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

    # Extract strike from question
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
# Book snapshot (unchanged — used by polymarket_ws for REST init)
# ---------------------------------------------------------------------------


async def fetch_book_snapshot(
    clob_url: str,
    token_id: str,
) -> tuple[Optional[float], Optional[float]]:
    """Fetch current best bid/ask from CLOB REST API.

    Returns (best_bid, best_ask) or (None, None) on failure.
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
                    return None, None
                raw = await resp.read()
                book = orjson.loads(raw)
        except Exception:
            log.exception("Failed to fetch book for %s", token_id[:12])
            return None, None

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    # Polymarket CLOB sorts bids ascending (worst→best) and asks descending (worst→best)
    # So best bid = last bid, best ask = last ask
    best_bid = float(bids[-1]["price"]) if bids else None
    best_ask = float(asks[-1]["price"]) if asks else None

    return best_bid, best_ask


