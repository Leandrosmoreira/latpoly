---
name: latpoly-strategy
description: Create a new trading strategy for latpoly or modify an existing one (S1-S4). Use when the user asks to create strategy_N, add entry/exit logic, modify zscore thresholds, cycle PnL targets, inventory control rules, or side blocking behavior.
allowed-tools: Read, Grep, Glob, Edit, Write
argument-hint: "[strategy number or name]"
---

# Latpoly Strategy Builder

You are creating or modifying a trading strategy for the latpoly Polymarket bot.

## MANDATORY: Read Before Writing

Before creating or modifying ANY strategy, you MUST read these files:
1. `src/latpoly/strategy/base.py` — BaseStrategy interface (the contract)
2. `src/latpoly/strategy/config.py` — StrategyConfig (shared config)
3. `src/latpoly/strategy/registry.py` — Where to register
4. The closest existing strategy for reference (usually `strategy_2.py`)

## BaseStrategy Contract

```python
class BaseStrategy(ABC):
    def __init__(self, cfg: StrategyConfig): self.cfg = cfg
    def on_tick(self, tick: dict, tick_idx: int) -> Signal: ...  # REQUIRED
    def reset_daily(self) -> None: ...
    def reset_trade(self) -> None: ...
    def notify_trade_result(self, pnl: float) -> None: ...
```

## Signal Dataclass

```python
@dataclass
class Signal:
    action: str          # "BUY_YES" | "BUY_NO" | "NONE"
    side: str            # "YES" | "NO" | ""
    reason: str          # Human-readable reason for logging
    entry_price: float   # Maker entry price (best_bid)
    exit_target: float   # Informational (live_trader uses fixed_exit_ticks)
    size: int            # Number of shares (default 6)
    net_edge: float      # Optional edge score
    time_weight: float   # Optional time weighting
    tick_idx: int         # Tick index for cooldown tracking
```

## Strategy Template — Follow This Structure

```python
"""Strategy N — [Name].

[1-line description of what makes it different]

Config (env vars):
  LATPOLY_SN_ORDER_SIZE = 6
  LATPOLY_SN_PROFIT_TICKS = 2
  LATPOLY_SN_ZSCORE_THRESHOLD = 1.5
  ...
"""
from __future__ import annotations
import logging, os
from latpoly.strategy.base import BaseStrategy
from latpoly.strategy.config import StrategyConfig
from latpoly.strategy.engine import Signal

log = logging.getLogger(__name__)

def _env_float(key, default): return float(os.environ.get(key, str(default)))
def _env_int(key, default): return int(os.environ.get(key, str(default)))

class StrategyNEngine(BaseStrategy):
    def __init__(self, cfg: StrategyConfig) -> None:
        super().__init__(cfg)
        # Strategy-specific config from LATPOLY_SN_* env vars
        self._order_size = _env_int("LATPOLY_SN_ORDER_SIZE", 6)
        # ... more config

        # Cycle state
        self._status = "WORKING"  # WORKING | DONE
        self._realized_pnl_cycle = 0.0
        self._last_condition_id = ""
        self._last_entry_tick_idx = -9999

    def on_tick(self, tick: dict, tick_idx: int) -> Signal:
        # 1. Market rotation check
        cid = tick.get("condition_id", "")
        if cid and cid != self._last_condition_id:
            if self._last_condition_id:
                self._reset_cycle(cid)
            self._last_condition_id = cid

        # 2. DONE gate
        if self._status == "DONE":
            return Signal(action="NONE", side="", reason="cycle_target_hit")

        # 3. Kill switches (daily limits from self.cfg)
        # 4. Time-to-expiry gate
        # 5. Entry signal logic → _check_entry()
        return self._check_entry(tick, tick_idx)

    def _check_entry(self, tick, tick_idx) -> Signal:
        # Standard 13-step filter chain:
        # 1. low_liquidity
        # 2. data freshness (age_binance_ms, age_poly_ms)
        # 3. distance_to_strike
        # 4. cooldown (tick_idx - last_entry)
        # 5. Binance lag signal (zscore_bn_move + bn_move_since_poly)
        # 6. Side determination (YES if bn_move>0, NO if <0)
        # 7. Min BTC move
        # 8. Momentum confirmation (optional)
        # 9. Probability range (mid_yes/mid_no)
        # 10. Spread check
        # 11. Depth check
        # 12. Entry price (best_bid)
        # 13. Size
        ...

    def notify_trade_result(self, pnl: float) -> None:
        self._realized_pnl_cycle += pnl
        # Check cycle target
        if self._realized_pnl_cycle >= self._cycle_target_pnl:
            self._status = "DONE"

    def reset_trade(self) -> None:
        # Clear per-trade state, PRESERVE cycle state
        self._last_entry_tick_idx = -9999

    def reset_daily(self) -> None:
        self._daily_trade_count = 0
        self._daily_pnl = 0.0

    def _reset_cycle(self, new_cid: str) -> None:
        # FULL reset: status=WORKING, pnl=0, all counters=0
        ...
```

## EXISTING STRATEGIES

| Name | Class | Key Difference |
|------|-------|----------------|
| `scalp` | StrategyEngine | Base 2-tick scalp, multi-position capable |
| `cycle_tp10` | CycleTP10Engine (S2) | Cycle PnL 10%, single position, Binance lag |
| `strategy_3` | Strategy3Engine | 1-tick profit, rapid re-entry at same level |
| `strategy_4` | Strategy4Engine | Inventory control, side blocking, time gates |

## REGISTRATION CHECKLIST

After creating `strategy_N.py`:
1. Add import in `registry.py`: `from latpoly.strategy.strategy_N import StrategyNEngine`
2. Add to dict: `"strategy_N": StrategyNEngine`
3. Test import: `python -c "from latpoly.strategy.registry import STRATEGIES; print(STRATEGIES)"`

## RULES

- Use `LATPOLY_S{N}_*` env vars for ALL config (never hardcode)
- Every entry signal MUST log with `$$$` prefix (live_trader expects it)
- All prices MUST be `round(x, 2)` — tick size is $0.01
- `on_tick()` must be fast (<1ms) — no I/O, no awaits
- `reset_trade()` preserves cycle state, only clears per-trade data
- `_reset_cycle()` clears EVERYTHING on market rotation
- Strategy NEVER places orders — it returns a Signal, live_trader executes
