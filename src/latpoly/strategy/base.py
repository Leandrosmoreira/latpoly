"""Abstract base class for all trading strategies.

Every strategy must implement on_tick() and reset_daily().
Input: normalized tick dict (Binance + Polymarket fused data).
Output: Signal (BUY_YES, BUY_NO, EXIT, HOLD_TO_EXPIRY, NONE).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from latpoly.strategy.config import StrategyConfig

if TYPE_CHECKING:
    from latpoly.strategy.engine import Signal


class BaseStrategy(ABC):
    """Interface that all strategy engines must implement."""

    def __init__(self, cfg: StrategyConfig) -> None:
        self.cfg = cfg

    @abstractmethod
    def on_tick(self, tick: dict, tick_idx: int) -> Signal:
        """Process one tick and return a trading signal."""

    @abstractmethod
    def reset_daily(self) -> None:
        """Reset daily counters (called at UTC midnight)."""

    def reset_trade(self) -> None:
        """Reset after a single trade completes (SELL filled or gave up).

        Default: full re-init (same as creating a new instance).
        Override in strategies that need to preserve state across trades
        (e.g. cycle PnL accumulation).
        """
        self.__init__(self.cfg)  # type: ignore[misc]

    def notify_trade_result(self, pnl: float) -> None:
        """Called by LiveTrader when a trade completes with realized PnL.

        Default: no-op. Override to track cumulative PnL across trades.
        """
