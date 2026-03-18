"""W2 — Polymarket worker: discovery + REST snapshot + WebSocket + market rotation."""

from __future__ import annotations

import asyncio
import logging
import time

import orjson
import websockets
import websockets.asyncio.client as ws_client

from latpoly.config import Config
from latpoly.shared_state import MarketInfo, SharedState
from latpoly.utils.discovery import discover_btc_15m_market, fetch_book_snapshot

log = logging.getLogger(__name__)

PING_INTERVAL = 8.0  # seconds (Polymarket requires < 10s)
PING_TIMEOUT = 20.0


async def _load_rest_snapshot(cfg: Config, state: SharedState, market: MarketInfo) -> None:
    """Fetch REST book snapshot for YES and NO tokens and update state."""
    log.info("Fetching REST snapshot for market %s", market.condition_id[:12])

    yes_bid, yes_ask, yes_bids_lvl, yes_asks_lvl = await fetch_book_snapshot(
        cfg.poly_rest_url, market.yes_token_id
    )
    no_bid, no_ask, no_bids_lvl, no_asks_lvl = await fetch_book_snapshot(
        cfg.poly_rest_url, market.no_token_id
    )

    pm = state.polymarket
    now_ns = time.time_ns()

    if yes_bid is not None:
        pm.yes_best_bid = yes_bid
    if yes_ask is not None:
        pm.yes_best_ask = yes_ask
    if no_bid is not None:
        pm.no_best_bid = no_bid
    if no_ask is not None:
        pm.no_best_ask = no_ask

    # Store book depth levels
    pm.yes_bids_levels = yes_bids_lvl
    pm.yes_asks_levels = yes_asks_lvl
    pm.no_bids_levels = no_bids_lvl
    pm.no_asks_levels = no_asks_lvl

    # Compute mids and spreads
    if pm.yes_best_bid is not None and pm.yes_best_ask is not None:
        pm.mid_yes = (pm.yes_best_bid + pm.yes_best_ask) / 2.0
        pm.spread_yes = pm.yes_best_ask - pm.yes_best_bid
    if pm.no_best_bid is not None and pm.no_best_ask is not None:
        pm.mid_no = (pm.no_best_bid + pm.no_best_ask) / 2.0
        pm.spread_no = pm.no_best_ask - pm.no_best_bid

    pm.ts_local_recv_ns = now_ns
    pm.ts_mono_ns = time.monotonic_ns()

    log.info(
        "REST snapshot: YES bid=%.4f ask=%.4f (%d lvls) | NO bid=%.4f ask=%.4f (%d lvls)",
        yes_bid or 0, yes_ask or 0, len(yes_asks_lvl),
        no_bid or 0, no_ask or 0, len(no_asks_lvl),
    )


def _update_side(pm, market, asset_id: str, best_bid, best_ask) -> None:
    """Apply best_bid/best_ask update for a specific asset."""
    if asset_id == market.yes_token_id:
        if best_bid is not None:
            pm.yes_best_bid = float(best_bid)
        if best_ask is not None:
            pm.yes_best_ask = float(best_ask)
        if pm.yes_best_bid is not None and pm.yes_best_ask is not None:
            pm.mid_yes = (pm.yes_best_bid + pm.yes_best_ask) / 2.0
            pm.spread_yes = pm.yes_best_ask - pm.yes_best_bid
    elif asset_id == market.no_token_id:
        if best_bid is not None:
            pm.no_best_bid = float(best_bid)
        if best_ask is not None:
            pm.no_best_ask = float(best_ask)
        if pm.no_best_bid is not None and pm.no_best_ask is not None:
            pm.mid_no = (pm.no_best_bid + pm.no_best_ask) / 2.0
            pm.spread_no = pm.no_best_ask - pm.no_best_bid


def _handle_price_change(data: dict, state: SharedState) -> None:
    """Handle price_change event — updates best bid/ask for each token.

    Polymarket WS sends price_changes as an array inside the event:
    {"price_changes": [{"asset_id": "...", "best_bid": "0.67", "best_ask": "0.72", ...}]}
    """
    pm = state.polymarket
    market = pm.market
    now_ns = time.time_ns()

    # price_changes is an array of per-asset updates
    for change in data.get("price_changes", []):
        asset_id = change.get("asset_id", "")
        best_bid = change.get("best_bid")
        best_ask = change.get("best_ask")
        _update_side(pm, market, asset_id, best_bid, best_ask)

    pm.ts_local_recv_ns = now_ns
    pm.ts_mono_ns = time.monotonic_ns()
    pm.event_count += 1


def _handle_book(data: dict, state: SharedState) -> None:
    """Handle book snapshot event — consistency reset.

    Polymarket CLOB sorts bids ascending (worst→best) and asks descending (worst→best).
    So best bid = last bid, best ask = last ask.
    """
    pm = state.polymarket
    market = pm.market
    asset_id = data.get("asset_id", "")
    now_ns = time.time_ns()

    bids = data.get("bids", [])
    asks = data.get("asks", [])

    # Bids ascending: last = highest = best bid
    # Asks descending: last = lowest = best ask
    try:
        best_bid = float(bids[-1]["price"]) if bids else None
    except (ValueError, TypeError, KeyError):
        best_bid = None
    try:
        best_ask = float(asks[-1]["price"]) if asks else None
    except (ValueError, TypeError, KeyError):
        best_ask = None

    _update_side(pm, market, asset_id, best_bid, best_ask)

    # Store full book depth levels (reversed: best first, top 10)
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

    if asset_id == market.yes_token_id:
        pm.yes_bids_levels = bids_levels[:10]
        pm.yes_asks_levels = asks_levels[:10]
    elif asset_id == market.no_token_id:
        pm.no_bids_levels = bids_levels[:10]
        pm.no_asks_levels = asks_levels[:10]

    pm.ts_local_recv_ns = now_ns
    pm.ts_mono_ns = time.monotonic_ns()
    pm.event_count += 1


def _handle_last_trade(data: dict, state: SharedState) -> None:
    """Handle last_trade_price event."""
    pm = state.polymarket
    market = pm.market
    asset_id = data.get("asset_id", "")
    price = data.get("price")

    if price is None:
        return

    price_f = float(price)
    if asset_id == market.yes_token_id:
        pm.last_trade_price_yes = price_f
    elif asset_id == market.no_token_id:
        pm.last_trade_price_no = price_f

    pm.ts_local_recv_ns = time.time_ns()
    pm.ts_mono_ns = time.monotonic_ns()
    pm.event_count += 1


_EVENT_HANDLERS = {
    "price_change": _handle_price_change,
    "book": _handle_book,
    "last_trade_price": _handle_last_trade,
}


async def _ping_loop(ws: object, shutdown: asyncio.Event) -> None:
    """Send PING every PING_INTERVAL seconds."""
    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=PING_INTERVAL)
            break
        except asyncio.TimeoutError:
            try:
                await ws.send("PING")  # type: ignore[union-attr]
            except Exception:
                break


async def _run_ws_session(
    cfg: Config,
    state: SharedState,
    market: MarketInfo,
) -> str:
    """Run a single WebSocket session. Returns reason for exit: 'rotation' | 'error' | 'shutdown'."""
    pm = state.polymarket

    try:
        async with ws_client.connect(
            cfg.poly_ws_url,
            ping_interval=None,  # we handle pings manually with literal "PING"
            close_timeout=5.0,
            max_size=2**20,
        ) as ws:
            log.info("Polymarket WS connected")
            pm.reconnect_count += 1

            # Subscribe to market
            sub_msg = orjson.dumps({
                "assets_ids": [market.yes_token_id, market.no_token_id],
                "type": "market",
            })
            await ws.send(sub_msg)
            log.info("Subscribed to YES=%s NO=%s", market.yes_token_id[:12], market.no_token_id[:12])

            # Start ping task
            ping_task = asyncio.create_task(_ping_loop(ws, state.shutdown))

            try:
                async for raw in ws:
                    if state.shutdown.is_set():
                        return "shutdown"

                    # Check market rotation
                    now = time.time()
                    if market.end_ts_s and now >= market.end_ts_s:
                        log.info("Market expired, rotating")
                        return "rotation"

                    # Handle message
                    if raw == "PONG" or raw == b"PONG":
                        continue

                    try:
                        msg = orjson.loads(raw)
                    except Exception:
                        continue

                    # Messages can be a list of events
                    events = msg if isinstance(msg, list) else [msg]
                    for event in events:
                        event_type = event.get("event_type", "") or event.get("type", "")
                        handler = _EVENT_HANDLERS.get(event_type)
                        if handler:
                            handler(event, state)

            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    except (
        websockets.exceptions.ConnectionClosed,
        websockets.exceptions.WebSocketException,
        OSError,
        asyncio.TimeoutError,
    ) as exc:
        if state.shutdown.is_set():
            return "shutdown"
        log.warning("Polymarket WS error: %s", exc)
        return "error"

    return "error"


async def polymarket_worker(cfg: Config, state: SharedState) -> None:
    """Main Polymarket worker loop: discover -> REST snapshot -> WS -> rotate."""
    log.info("Polymarket worker starting")
    backoff = 0.5
    max_backoff = 30.0

    try:
        while not state.shutdown.is_set():
            # Phase A: Discovery
            log.info("Running market discovery...")
            market = await discover_btc_15m_market(cfg.gamma_api_url, cfg.poly_rest_url)

            if market is None:
                log.warning("No market found, retrying in %.1fs", backoff)
                try:
                    await asyncio.wait_for(state.shutdown.wait(), timeout=backoff)
                    break
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, max_backoff)
                    continue

            backoff = 0.5  # reset on success

            # Use Binance mid as strike if not extracted from question text
            # (up/down markets don't have a dollar strike in the title)
            if market.strike == 0.0 and state.binance.mid is not None:
                market.strike = round(state.binance.mid, 2)
                log.info("Strike set from Binance mid: %.2f", market.strike)

            state.polymarket.market = market

            # Phase B: REST snapshot
            await _load_rest_snapshot(cfg, state, market)

            # Retry strike if Binance wasn't ready during discovery
            # (wait up to 5s for Binance to be ready)
            if market.strike == 0.0:
                for _ in range(10):
                    if state.binance.mid is not None:
                        market.strike = round(state.binance.mid, 2)
                        log.info("Strike set from Binance mid (deferred): %.2f", market.strike)
                        break
                    await asyncio.sleep(0.5)
                if market.strike == 0.0:
                    log.warning("Strike still 0.0 — Binance not ready after 5s")

            # Phase C: WebSocket session
            reason = await _run_ws_session(cfg, state, market)

            if reason == "shutdown":
                break
            elif reason == "rotation":
                log.info("Market rotation — discovering next market")
                continue
            else:  # error
                log.warning("WS session ended, reconnecting in %.1fs", backoff)
                try:
                    await asyncio.wait_for(state.shutdown.wait(), timeout=backoff)
                    break
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, max_backoff)

    except asyncio.CancelledError:
        log.info("Polymarket worker cancelled")

    log.info("Polymarket worker stopped")
