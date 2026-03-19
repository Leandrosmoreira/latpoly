"""W2 — Polymarket worker: discovery + REST snapshot + WebSocket + market rotation.

Multi-market: one worker per MarketSlotDef.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

import orjson
import websockets
import websockets.asyncio.client as ws_client

from latpoly.config import Config
from latpoly.shared_state import MarketInfo, PolymarketState, SharedState
from latpoly.utils.discovery import discover_market, fetch_book_snapshot

if TYPE_CHECKING:
    from latpoly.config import MarketSlotDef

log = logging.getLogger(__name__)

PING_INTERVAL = 8.0
PING_TIMEOUT = 20.0


async def _load_rest_snapshot(
    cfg: Config, pm: PolymarketState, market: MarketInfo, slot_id: str,
) -> None:
    """Fetch REST book snapshot for YES and NO tokens and update state."""
    log.info("[%s] Fetching REST snapshot for %s", slot_id, market.condition_id[:12])

    yes_bid, yes_ask, yes_bids_lvl, yes_asks_lvl = await fetch_book_snapshot(
        cfg.poly_rest_url, market.yes_token_id
    )
    no_bid, no_ask, no_bids_lvl, no_asks_lvl = await fetch_book_snapshot(
        cfg.poly_rest_url, market.no_token_id
    )

    now_ns = time.time_ns()

    if yes_bid is not None:
        pm.yes_best_bid = yes_bid
    if yes_ask is not None:
        pm.yes_best_ask = yes_ask
    if no_bid is not None:
        pm.no_best_bid = no_bid
    if no_ask is not None:
        pm.no_best_ask = no_ask

    pm.yes_bids_levels = yes_bids_lvl
    pm.yes_asks_levels = yes_asks_lvl
    pm.no_bids_levels = no_bids_lvl
    pm.no_asks_levels = no_asks_lvl

    if pm.yes_best_bid is not None and pm.yes_best_ask is not None:
        pm.mid_yes = (pm.yes_best_bid + pm.yes_best_ask) / 2.0
        pm.spread_yes = pm.yes_best_ask - pm.yes_best_bid
    if pm.no_best_bid is not None and pm.no_best_ask is not None:
        pm.mid_no = (pm.no_best_bid + pm.no_best_ask) / 2.0
        pm.spread_no = pm.no_best_ask - pm.no_best_bid

    pm.ts_local_recv_ns = now_ns
    pm.ts_mono_ns = time.monotonic_ns()

    log.info(
        "[%s] REST snapshot: YES bid=%.4f ask=%.4f (%d lvls) | NO bid=%.4f ask=%.4f (%d lvls)",
        slot_id,
        yes_bid or 0, yes_ask or 0, len(yes_asks_lvl),
        no_bid or 0, no_ask or 0, len(no_asks_lvl),
    )


def _update_side(pm: PolymarketState, market: MarketInfo, asset_id: str, best_bid, best_ask) -> None:
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


def _handle_price_change(data: dict, pm: PolymarketState) -> None:
    """Handle price_change event."""
    market = pm.market
    now_ns = time.time_ns()

    for change in data.get("price_changes", []):
        asset_id = change.get("asset_id", "")
        best_bid = change.get("best_bid")
        best_ask = change.get("best_ask")
        _update_side(pm, market, asset_id, best_bid, best_ask)

    pm.ts_local_recv_ns = now_ns
    pm.ts_mono_ns = time.monotonic_ns()
    pm.event_count += 1


def _handle_book(data: dict, pm: PolymarketState) -> None:
    """Handle book snapshot event."""
    market = pm.market
    asset_id = data.get("asset_id", "")
    now_ns = time.time_ns()

    bids = data.get("bids", [])
    asks = data.get("asks", [])

    try:
        best_bid = float(bids[-1]["price"]) if bids else None
    except (ValueError, TypeError, KeyError):
        best_bid = None
    try:
        best_ask = float(asks[-1]["price"]) if asks else None
    except (ValueError, TypeError, KeyError):
        best_ask = None

    _update_side(pm, market, asset_id, best_bid, best_ask)

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


def _handle_last_trade(data: dict, pm: PolymarketState) -> None:
    """Handle last_trade_price event."""
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
    pm: PolymarketState,
    market: MarketInfo,
    slot_id: str,
) -> str:
    """Run a single WebSocket session. Returns 'rotation' | 'error' | 'shutdown'."""
    try:
        async with ws_client.connect(
            cfg.poly_ws_url,
            ping_interval=None,
            close_timeout=5.0,
            max_size=2**20,
        ) as ws:
            log.info("[%s] Polymarket WS connected", slot_id)
            pm.reconnect_count += 1

            sub_msg = orjson.dumps({
                "assets_ids": [market.yes_token_id, market.no_token_id],
                "type": "market",
            })
            await ws.send(sub_msg)
            log.info("[%s] Subscribed YES=%s NO=%s",
                     slot_id, market.yes_token_id[:12], market.no_token_id[:12])

            ping_task = asyncio.create_task(_ping_loop(ws, state.shutdown))

            try:
                async for raw in ws:
                    if state.shutdown.is_set():
                        return "shutdown"

                    now = time.time()
                    if market.end_ts_s and now >= market.end_ts_s:
                        log.info("[%s] Market expired, rotating", slot_id)
                        return "rotation"

                    if raw == "PONG" or raw == b"PONG":
                        continue

                    try:
                        msg = orjson.loads(raw)
                    except Exception:
                        continue

                    events = msg if isinstance(msg, list) else [msg]
                    for event in events:
                        event_type = event.get("event_type", "") or event.get("type", "")
                        handler = _EVENT_HANDLERS.get(event_type)
                        if handler:
                            handler(event, pm)

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
        log.warning("[%s] Polymarket WS error: %s", slot_id, exc)
        return "error"

    return "error"


async def polymarket_slot_worker(
    cfg: Config, state: SharedState, slot_def: MarketSlotDef,
) -> None:
    """Polymarket worker for a single market slot: discover -> REST -> WS -> rotate."""
    slot_id = slot_def.slot_id
    pm = state.get_polymarket(slot_id)
    log.info("[%s] Polymarket slot worker starting", slot_id)
    backoff = 0.5
    max_backoff = 30.0

    try:
        while not state.shutdown.is_set():
            # Phase A: Discovery
            log.info("[%s] Running market discovery...", slot_id)
            market = await discover_market(
                cfg.gamma_api_url, cfg.poly_rest_url, slot_def,
            )

            if market is None:
                log.warning("[%s] No market found, retrying in %.1fs", slot_id, backoff)
                try:
                    await asyncio.wait_for(state.shutdown.wait(), timeout=backoff)
                    break
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, max_backoff)
                    continue

            backoff = 0.5

            # Strike fallback from Binance mid
            if market.strike == 0.0:
                bn = state.get_binance(slot_def.binance_symbol)
                if bn.mid is not None:
                    market.strike = round(bn.mid, 2)
                    log.info("[%s] Strike set from Binance mid: %.2f",
                             slot_id, market.strike)

            pm.market = market

            # Phase B: REST snapshot
            await _load_rest_snapshot(cfg, pm, market, slot_id)

            # Deferred strike from Binance
            if market.strike == 0.0:
                bn = state.get_binance(slot_def.binance_symbol)
                for _ in range(10):
                    if bn.mid is not None:
                        market.strike = round(bn.mid, 2)
                        log.info("[%s] Strike set from Binance (deferred): %.2f",
                                 slot_id, market.strike)
                        break
                    await asyncio.sleep(0.5)

            # Phase C: WebSocket session
            reason = await _run_ws_session(cfg, state, pm, market, slot_id)

            if reason == "shutdown":
                break
            elif reason == "rotation":
                log.info("[%s] Market rotation — discovering next", slot_id)
                continue
            else:
                log.warning("[%s] WS session ended, reconnecting in %.1fs",
                            slot_id, backoff)
                try:
                    await asyncio.wait_for(state.shutdown.wait(), timeout=backoff)
                    break
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, max_backoff)

    except asyncio.CancelledError:
        log.info("[%s] Polymarket slot worker cancelled", slot_id)

    log.info("[%s] Polymarket slot worker stopped", slot_id)


# Backward-compat wrapper
async def polymarket_worker(cfg: Config, state: SharedState) -> None:
    """Legacy single-market worker (backward-compat)."""
    from latpoly.config import MarketSlotDef
    slot = MarketSlotDef("btc-15m", "btcusdt", "btc", "15m", 900, "timestamp")
    await polymarket_slot_worker(cfg, state, slot)
