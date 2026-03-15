"""Polymarket market discovery via Gamma API."""

from __future__ import annotations

import logging
import time
from typing import Optional

import aiohttp
import orjson

from latpoly.shared_state import MarketInfo

log = logging.getLogger(__name__)

# Keywords to match BTC 15-minute markets
_BTC_KEYWORDS = ("bitcoin", "btc")
_15M_KEYWORDS = ("15-minute", "15 minute", "15min", "15m")


async def discover_btc_15m_market(
    gamma_url: str,
    clob_url: str,
) -> Optional[MarketInfo]:
    """Find the current active BTC 15-minute prediction market.

    Queries the Gamma API for active events, filters for BTC 15m markets,
    and returns the one expiring soonest (the current active one).
    """
    now = time.time()

    async with aiohttp.ClientSession() as session:
        # Search for active BTC markets
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
            "order": "endDate",
            "ascending": "true",
        }
        try:
            async with session.get(
                f"{gamma_url}/events", params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    log.error("Gamma API returned %d", resp.status)
                    return None
                raw = await resp.read()
                events = orjson.loads(raw)
        except Exception:
            log.exception("Failed to query Gamma API")
            return None

        # Filter for BTC 15-minute markets
        candidates: list[dict] = []
        for event in events:
            title = (event.get("title", "") or "").lower()
            slug = (event.get("slug", "") or "").lower()
            desc = (event.get("description", "") or "").lower()
            combined = f"{title} {slug} {desc}"

            is_btc = any(kw in combined for kw in _BTC_KEYWORDS)
            is_15m = any(kw in combined for kw in _15M_KEYWORDS)

            if is_btc and is_15m:
                candidates.append(event)

        if not candidates:
            log.warning("No BTC 15m markets found in %d events", len(events))
            return None

        # Find the current active market (expiring soonest but still in the future)
        best: Optional[dict] = None
        best_end: float = float("inf")

        for ev in candidates:
            # Get markets (sub-markets) within the event
            markets = ev.get("markets", [])
            if not markets:
                continue

            for mkt in markets:
                if mkt.get("closed") or not mkt.get("active"):
                    continue

                end_str = mkt.get("endDate") or ev.get("endDate", "")
                if not end_str:
                    continue

                # Parse ISO date to epoch
                try:
                    from datetime import datetime, timezone

                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    end_ts = end_dt.timestamp()
                except Exception:
                    continue

                if end_ts > now and end_ts < best_end:
                    best_end = end_ts
                    best = mkt
                    best["_event"] = ev
                    best["_end_ts"] = end_ts

        if best is None:
            log.warning("No active BTC 15m market expiring in the future")
            return None

        # Extract token IDs
        clob_token_ids = best.get("clobTokenIds", [])
        outcomes = best.get("outcomes", [])
        outcome_prices = best.get("outcomePrices", [])

        yes_token = clob_token_ids[0] if len(clob_token_ids) > 0 else ""
        no_token = clob_token_ids[1] if len(clob_token_ids) > 1 else ""

        # Try to extract strike from title/question
        strike = _extract_strike(best.get("question", "") or best.get("_event", {}).get("title", ""))

        info = MarketInfo(
            condition_id=best.get("conditionId", "") or best.get("condition_id", ""),
            slug=best.get("_event", {}).get("slug", ""),
            yes_token_id=yes_token,
            no_token_id=no_token,
            end_ts_s=best["_end_ts"],
            strike=strike,
            question=best.get("question", ""),
        )

        log.info(
            "Discovered market: %s | strike=%.1f | ends=%s | yes=%s no=%s",
            info.question[:60],
            info.strike,
            time.strftime("%H:%M:%S", time.gmtime(info.end_ts_s)),
            info.yes_token_id[:12],
            info.no_token_id[:12],
        )

        return info


def _extract_strike(text: str) -> float:
    """Try to extract a BTC price strike from market question text.

    Examples:
    - "Will Bitcoin be above $85,000 at 14:30 UTC?" -> 85000.0
    - "BTC above 84500.50?" -> 84500.5
    """
    import re

    # Match dollar amounts like $85,000 or $84500.50
    match = re.search(r"\$?([\d,]+(?:\.\d+)?)", text.replace(",", ""))
    if match:
        try:
            val = float(match.group(1))
            # Sanity check: BTC price should be in a reasonable range
            if 10_000 < val < 500_000:
                return val
        except ValueError:
            pass
    return 0.0


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
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    log.warning("CLOB book API returned %d for %s", resp.status, token_id[:12])
                    return None, None
                raw = await resp.read()
                book = orjson.loads(raw)
        except Exception:
            log.exception("Failed to fetch book for %s", token_id[:12])
            return None, None

    # Parse best bid/ask from book response
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None

    return best_bid, best_ask
