"""Strategy registry — maps strategy names to engine classes.

Usage:
    from latpoly.strategy.registry import get_strategy
    engine_cls = get_strategy("scalp")
    engine = engine_cls(strat_cfg)

Select strategy via env var: LATPOLY_STRATEGY=scalp (default)
"""

from __future__ import annotations

from latpoly.strategy.base import BaseStrategy
from latpoly.strategy.engine import StrategyEngine
from latpoly.strategy.strategy_2 import CycleTP10Engine
from latpoly.strategy.strategy_3 import Strategy3Engine

# Register all available strategies here.
# To add a new strategy:
#   1. Create strategy_X.py with a class that inherits BaseStrategy
#   2. Import it here
#   3. Add it to the STRATEGIES dict
STRATEGIES: dict[str, type[BaseStrategy]] = {
    "scalp": StrategyEngine,
    "cycle_tp10": CycleTP10Engine,
    "strategy_3": Strategy3Engine,
}


def get_strategy(name: str) -> type[BaseStrategy]:
    """Return the strategy class for the given name."""
    if name not in STRATEGIES:
        available = ", ".join(sorted(STRATEGIES))
        raise ValueError(
            f"Unknown strategy {name!r}. Available: {available}"
        )
    return STRATEGIES[name]
