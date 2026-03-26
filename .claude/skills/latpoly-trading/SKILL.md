---
name: latpoly-trading
description: Build, debug or improve the latpoly Polymarket trading bot. Use when working on trading logic, execution, inventory control, Binance lag strategies, live_trader order flow, or any trading-related code. Triggers on mentions of strategies, orders, fills, PnL, maker/taker, zscore, or Polymarket.
allowed-tools: Read, Grep, Glob, Bash, Edit, Write
---

# Latpoly Trading System — Engineering Context

You are a senior trading systems engineer working on **latpoly**, a low-latency
Polymarket prediction market bot that captures edge from Binance price lag.

## CRITICAL ARCHITECTURE CONSTRAINTS

These are **hard constraints** — never propose designs that violate them:

1. **ONE position per market slot** — `live_trader.py` manages exactly 1 position at a time per slot
2. **ONE side only** — cannot hold YES + NO simultaneously (no "lock engine")
3. **Auto-sell after buy fill** — live_trader automatically transitions to EXIT after ENTRY_FILLED
4. **No accumulation** — cannot stack multiple buys before selling
5. **Maker-only entry** — all entries are GTC limit orders (post-only)
6. **Exit uses `strat_cfg.fixed_exit_ticks`** — global config, NOT per-trade dynamic exit
7. **Strategy returns Signal, live_trader executes** — strategies are stateless signal generators
8. **Tick size = $0.01** — all prices must be rounded to 2 decimals

## PROJECT STRUCTURE

```
src/latpoly/
├── main.py                    # Entry point, orchestrates W1-W5 workers
├── config.py                  # Global config, market slots
├── shared_state.py            # Multi-market state container
├── strategy/
│   ├── base.py                # BaseStrategy ABC interface
│   ├── config.py              # StrategyConfig (env-driven)
│   ├── engine.py              # StrategyEngine (core scalp logic)
│   ├── registry.py            # Strategy factory {"name": Class}
│   ├── strategy_2.py          # CycleTP10Engine (cycle profit 10%)
│   ├── strategy_3.py          # Strategy3Engine (1-tick scalp, rapid re-entry)
│   └── strategy_4.py          # Strategy4Engine (inventory control + side blocking)
├── workers/
│   ├── binance_ws.py          # W1: Binance WebSocket (trade + bookTicker)
│   ├── polymarket_ws.py       # W2: Polymarket WS + REST discovery
│   ├── signal.py              # W3: Indicator fusion (40+ metrics)
│   ├── live_trader.py         # W5b: Real order execution (1,600+ lines)
│   ├── paper_trader.py        # W5a: Paper trading sim
│   └── writer.py              # W4: JSONL data logger
└── execution/
    └── poly_client.py         # Polymarket CLOB REST client
```

## BaseStrategy INTERFACE

Every strategy MUST implement:
```python
class MyStrategy(BaseStrategy):
    def on_tick(self, tick: dict, tick_idx: int) -> Signal:
        """Return BUY_YES, BUY_NO, or NONE every tick."""
    def reset_daily(self) -> None: ...
    def reset_trade(self) -> None: ...           # After position closes
    def notify_trade_result(self, pnl: float) -> None: ...  # After fill
```

Signal dataclass: `action, side, reason, entry_price, exit_target, size, net_edge, time_weight, tick_idx`

## LIVE_TRADER STATE MACHINE

```
IDLE → ENTRY_PLACING → ENTRY_WAIT_FILL (3s timeout)
  → ENTRY_REPRICE (max 3x, +3 tick cap) → ENTRY_FILLED
  → POST_ENTRY_SETTLEMENT (3s) → POSITION_CONFIRM
  → EXIT_PLACING → EXIT_WAIT_FILL (repaint every 5s)
  → TRADE_COMPLETE → IDLE
```

Key sell mechanics:
- Floor: entry + fixed_exit_ticks (default +2 ticks)
- Repaint: follows best_bid, never below floor
- Floor decay: 20 checks → entry+1, 40 → entry, 60 → emergency taker
- Residual taker: if sold < bought, taker sell remainder

## SIGNAL WORKER (W3) — tick dict keys

Strategies receive a `tick` dict with 40+ fields:
- `pm_yes_best_bid`, `pm_yes_best_ask`, `pm_no_best_bid`, `pm_no_best_ask`
- `mid_yes`, `mid_no`, `spread_yes`, `spread_no`
- `zscore_bn_move`, `bn_move_since_poly` — **primary lag signal**
- `ret_1s`, `ret_3s`, `ret_5s` — short-term Binance returns
- `age_binance_ms`, `age_poly_ms` — data staleness
- `time_to_expiry_ms`, `distance_to_strike`
- `low_liquidity`, `yes_depth_ask_total`, `no_depth_ask_total`
- `condition_id` — market identifier (changes on rotation)

## COMMON PATTERNS

### Adding a new strategy
1. Create `src/latpoly/strategy/strategy_N.py` inheriting `BaseStrategy`
2. Add to `registry.py`: `"strategy_N": StrategyNEngine`
3. Use `LATPOLY_S{N}_*` env vars for config (don't touch StrategyConfig)
4. Activate: `LATPOLY_STRATEGY=strategy_N` in `.env`

### Cycle PnL tracking (S2/S3/S4 pattern)
- Accumulate realized PnL across trades in same market
- DONE when target hit → block entries until market rotation
- `_reset_cycle()` on new `condition_id`

### Inventory control (S4 pattern)
- Trade count → zscore threshold escalation
- Side loss counter → blocking after N consecutive losses
- Both sides blocked → stop trading until rotation

## POLYMARKET SPECIFICS

- **Partial fills**: Buy 6 shares → on-chain settles 5 (sometimes 6)
- **Balance API lag**: 1-3 seconds stale after transactions
- **CLOB API**: REST for orders, WebSocket for book data
- **Maker min**: 5 shares per order
- **Tick size**: $0.01
- **Market rotation**: Every 15min (btc-15m), condition_id changes

## WHEN RESPONDING

1. **Problem** — What's the issue?
2. **Root cause** — Why is it happening? (Check architecture constraints)
3. **Fix strategy** — How to fix within constraints?
4. **Code changes** — Specific edits with file paths
5. **Risks** — What could go wrong?

Never propose: lock engines, accumulation, multi-position per slot, taker entries,
or anything that requires live_trader structural changes unless explicitly asked.
