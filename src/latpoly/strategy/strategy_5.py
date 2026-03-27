"""Strategy 5 — Informed Market Maker (Trend-Aware MM).

Extends the Avellaneda-Stoikov MM (mm_quote_engine) with Binance trend detection.
Instead of quoting blindly on both sides, uses Binance short-term returns to
classify the market trend and bias quotes accordingly:

  STRONG_UP  → only BUY YES side, block BUY NO (don't sell into a rally)
  MILD_UP    → both sides, but YES side tighter spread, NO side wider
  FLAT       → standard two-sided MM (equal both sides)
  MILD_DOWN  → both sides, but NO side tighter spread, YES side wider
  STRONG_DOWN→ only BUY NO side, block BUY YES (don't buy into a dump)

Also adds sell floor protection: never sells below avg_entry + min_profit_ticks.

Trend detection uses a weighted composite of:
  - ret_1s (weight 0.5) — fastest reaction
  - ret_3s (weight 0.3) — confirms direction
  - ret_5s (weight 0.2) — filters noise
  - zscore_bn_move       — outlier amplifier

Config (env vars):
  LATPOLY_S5_TREND_STRONG       = 0.0004  # ret composite for STRONG trend
  LATPOLY_S5_TREND_MILD         = 0.0001  # ret composite for MILD trend
  LATPOLY_S5_ZSCORE_AMPLIFY     = 1.0     # zscore threshold to amplify trend
  LATPOLY_S5_TREND_SPREAD_MULT  = 1.5     # spread multiplier on counter-trend side
  LATPOLY_S5_MIN_PROFIT_TICKS   = 2       # sell floor = avg_entry + N ticks
  LATPOLY_S5_TREND_BLOCK_INV    = 4       # block trend side if inv > this
  + all LATPOLY_MM_* params from mm_quote_engine
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass

from latpoly.strategy.mm_quote_engine import (
    MMParams,
    MMQuoteEngine,
    QuotePair,
    TICK_SIZE,
    MIN_PRICE,
    MAX_PRICE,
    SIGMA_MIN,
    _env_float,
    _env_int,
    _floor_tick,
    _ceil_tick,
    _clamp,
)


# ---------------------------------------------------------------------------
# Trend classification
# ---------------------------------------------------------------------------

TREND_STRONG_UP = "STRONG_UP"
TREND_MILD_UP = "MILD_UP"
TREND_FLAT = "FLAT"
TREND_MILD_DOWN = "MILD_DOWN"
TREND_STRONG_DOWN = "STRONG_DOWN"


@dataclass
class S5Params:
    """Strategy 5 specific parameters — on top of MMParams."""
    trend_strong: float = 0.0
    trend_mild: float = 0.0
    zscore_amplify: float = 0.0
    trend_spread_mult: float = 0.0
    min_profit_ticks: int = 0
    trend_block_inv: int = 0

    def __init__(self) -> None:
        self.trend_strong = _env_float("LATPOLY_S5_TREND_STRONG", 0.0004)
        self.trend_mild = _env_float("LATPOLY_S5_TREND_MILD", 0.0001)
        self.zscore_amplify = _env_float("LATPOLY_S5_ZSCORE_AMPLIFY", 1.0)
        self.trend_spread_mult = _env_float("LATPOLY_S5_TREND_SPREAD_MULT", 1.5)
        self.min_profit_ticks = _env_int("LATPOLY_S5_MIN_PROFIT_TICKS", 2)
        self.trend_block_inv = _env_int("LATPOLY_S5_TREND_BLOCK_INV", 4)


# ---------------------------------------------------------------------------
# Informed Quote Engine
# ---------------------------------------------------------------------------


class InformedMMQuoteEngine(MMQuoteEngine):
    """Avellaneda-Stoikov + Binance trend detection.

    Overrides compute_quotes() to add:
    1. Trend classification from Binance returns
    2. Side blocking / spread skewing based on trend
    3. Sell floor protection (never sell below avg_entry + min_profit)
    """

    def __init__(self, mm_params: MMParams, s5_params: S5Params) -> None:
        super().__init__(mm_params)
        self.s5 = s5_params

    # ------------------------------------------------------------------
    # Trend detection
    # ------------------------------------------------------------------

    @staticmethod
    def compute_trend_composite(tick: dict) -> float | None:
        """Weighted composite of Binance returns.

        Returns a single float: positive = up trend, negative = down trend.
        Weights: ret_1s (0.5), ret_3s (0.3), ret_5s (0.2).
        """
        ret_1s = tick.get("ret_1s")
        ret_3s = tick.get("ret_3s")
        ret_5s = tick.get("ret_5s")

        if ret_1s is None and ret_3s is None:
            return None

        composite = 0.0
        total_weight = 0.0

        if ret_1s is not None:
            composite += ret_1s * 0.5
            total_weight += 0.5
        if ret_3s is not None:
            composite += ret_3s * 0.3
            total_weight += 0.3
        if ret_5s is not None:
            composite += ret_5s * 0.2
            total_weight += 0.2

        if total_weight == 0:
            return None

        return composite / total_weight

    def classify_trend(self, tick: dict) -> str:
        """Classify market trend from Binance data.

        Uses ret composite + zscore amplifier.
        If zscore_bn_move > zscore_amplify threshold, promotes MILD → STRONG.
        """
        composite = self.compute_trend_composite(tick)
        if composite is None:
            return TREND_FLAT

        zscore = tick.get("zscore_bn_move")
        abs_zscore = abs(zscore) if zscore is not None else 0.0

        abs_composite = abs(composite)

        # Strong trend: composite above strong threshold, OR
        # mild composite + strong zscore confirmation
        if composite > 0:
            if abs_composite >= self.s5.trend_strong:
                return TREND_STRONG_UP
            if abs_composite >= self.s5.trend_mild:
                if abs_zscore >= self.s5.zscore_amplify:
                    return TREND_STRONG_UP
                return TREND_MILD_UP
            return TREND_FLAT
        else:
            if abs_composite >= self.s5.trend_strong:
                return TREND_STRONG_DOWN
            if abs_composite >= self.s5.trend_mild:
                if abs_zscore >= self.s5.zscore_amplify:
                    return TREND_STRONG_DOWN
                return TREND_MILD_DOWN
            return TREND_FLAT

    # ------------------------------------------------------------------
    # Override: compute_quotes with trend awareness
    # ------------------------------------------------------------------

    def compute_quotes(
        self,
        tick: dict,
        net_inventory: int,
        time_phase: str,
        inventory_yes: int = 0,
        inventory_no: int = 0,
        avg_entry_yes: float = 0.0,
        avg_entry_no: float = 0.0,
    ) -> QuotePair | None:
        """Compute trend-aware bid/ask quotes.

        Extends parent with:
        - Trend-based side blocking and spread skewing
        - Sell floor protection (avg_entry + min_profit_ticks)
        - Total inventory control (YES+NO, not just net)
        - Price extreme protection (don't quote near 0 or 1)

        Extra args vs parent:
            inventory_yes: total YES shares held
            inventory_no: total NO shares held
            avg_entry_yes: average entry price for YES inventory
            avg_entry_no: average entry price for NO inventory
        """
        # Halt = no quotes
        if time_phase == "halt":
            return None

        # Need mid price
        mid_yes = tick.get("mid_yes")
        if mid_yes is None or mid_yes <= 0:
            return None

        # Need time to expiry
        tte_ms = tick.get("time_to_expiry_ms")
        if tte_ms is None or tte_ms <= 0:
            return None

        # Data freshness check
        age_bn = tick.get("age_binance_ms")
        age_pm = tick.get("age_poly_ms")
        if age_bn is not None and age_bn > self.p.max_data_age_ms:
            return None
        if age_pm is not None and age_pm > self.p.max_data_age_ms:
            return None

        # Low liquidity = don't quote
        if tick.get("low_liquidity", False):
            return None

        # --- Step 1: Volatility + time ---
        sigma = self.estimate_sigma(tick)
        T = (tte_ms / 1000.0) / self.p.cycle_window_s
        T = max(T, 0.001)

        # --- Step 2: Trend classification ---
        trend = self.classify_trend(tick)

        # --- Step 3: Reservation price (Avellaneda-Stoikov) ---
        q = net_inventory
        reservation = mid_yes - q * self.p.gamma * (sigma ** 2) * T

        # --- Step 4: Base spread ---
        spread = self.p.gamma * (sigma ** 2) * T + self.p.base_spread_ticks * TICK_SIZE

        # --- Step 5: Adverse selection widening ---
        bn_move = tick.get("bn_move_since_poly")
        if bn_move is not None and abs(bn_move) > self.p.adverse_threshold:
            spread += self.p.adverse_extra_ticks * TICK_SIZE

        # --- Step 6: Time phase adjustments ---
        if time_phase == "reduce":
            spread *= 1.5
        elif time_phase == "exit":
            spread *= 2.0

        # Clamp spread
        spread = _clamp(
            spread,
            self.p.base_spread_ticks * TICK_SIZE,
            self.p.max_spread_ticks * TICK_SIZE,
        )

        # --- Step 7: Trend-based spread skewing ---
        # Split spread asymmetrically: tighter on trend side, wider against
        spread_yes = spread / 2.0  # half-spread for YES side (bid)
        spread_no = spread / 2.0   # half-spread for NO side (ask YES = bid NO)

        if trend in (TREND_MILD_UP, TREND_STRONG_UP):
            # Trend up: tighten YES bid (want to buy YES), widen NO bid
            spread_no *= self.s5.trend_spread_mult
        elif trend in (TREND_MILD_DOWN, TREND_STRONG_DOWN):
            # Trend down: tighten NO bid (want to buy NO), widen YES bid
            spread_yes *= self.s5.trend_spread_mult

        # --- Step 8: Compute bid/ask for YES ---
        bid_yes_raw = reservation - spread_yes
        ask_yes_raw = reservation + spread_no

        bid_yes_price = round(_floor_tick(bid_yes_raw), 2)
        ask_yes_price = round(_ceil_tick(ask_yes_raw), 2)

        # Ensure minimum 1 tick spread
        if ask_yes_price <= bid_yes_price:
            ask_yes_price = round(bid_yes_price + TICK_SIZE, 2)

        # --- Step 9: Sell floor protection ---
        # Never sell YES below avg_entry + min_profit
        if avg_entry_yes > 0:
            sell_floor_yes = round(avg_entry_yes + self.s5.min_profit_ticks * TICK_SIZE, 2)
            if ask_yes_price < sell_floor_yes:
                ask_yes_price = sell_floor_yes

        # --- Step 10: Convert to BID NO ---
        bid_no_price = round(1.0 - ask_yes_price, 2)

        # Never sell NO below avg_entry + min_profit (via bid_yes constraint)
        if avg_entry_no > 0:
            sell_floor_no = round(avg_entry_no + self.s5.min_profit_ticks * TICK_SIZE, 2)
            # SELL NO = BUY YES at (1 - sell_no_price)
            # bid_yes must be <= 1 - sell_floor_no for the exit to be profitable
            max_bid_yes_for_no_exit = round(1.0 - sell_floor_no, 2)
            if bid_yes_price > max_bid_yes_for_no_exit:
                bid_yes_price = max_bid_yes_for_no_exit

        # --- Price extreme protection ---
        # Don't buy YES above $0.90 or below $0.10
        # (near expiry, market goes to 0 or 1 and we'd get picked off)
        EXTREME_LOW = 0.10
        EXTREME_HIGH = 0.90
        bid_yes_price = round(_clamp(bid_yes_price, EXTREME_LOW, EXTREME_HIGH), 2)
        bid_no_price = round(_clamp(bid_no_price, EXTREME_LOW, EXTREME_HIGH), 2)

        # Final clamp within valid range
        bid_yes_price = round(_clamp(bid_yes_price, MIN_PRICE, MAX_PRICE), 2)
        bid_no_price = round(_clamp(bid_no_price, MIN_PRICE, MAX_PRICE), 2)

        # --- Step 11: Compute sizes with inventory + trend ---
        abs_inv = abs(net_inventory)
        total_inv = inventory_yes + inventory_no
        base_size = self.p.quote_size

        # Start with inventory-based sizing
        # Priority: exit > total hard > net hard > total soft > net soft > normal
        if time_phase == "exit":
            if net_inventory > 0:
                bid_yes_size = 0
                bid_no_size = min(base_size, net_inventory)
            elif net_inventory < 0:
                bid_yes_size = min(base_size, abs_inv)
                bid_no_size = 0
            else:
                bid_yes_size = 0
                bid_no_size = 0
        elif total_inv >= self.p.max_inventory * 2:
            # TOTAL inventory hard limit: stop buying both sides entirely
            # Only allow exits when we hold too much total
            bid_yes_size = 0
            bid_no_size = 0
        elif abs_inv >= self.p.max_inventory:
            # Net inventory hard limit: cancel entry side entirely
            if net_inventory > 0:
                bid_yes_size = 0
                bid_no_size = base_size
            else:
                bid_yes_size = base_size
                bid_no_size = 0
        elif total_inv >= self.p.max_inventory:
            # TOTAL soft limit: reduce both sides to minimum
            bid_yes_size = self.p.min_maker_size
            bid_no_size = self.p.min_maker_size
        elif abs_inv >= self.p.soft_inventory:
            # Net soft limit: reduce entry side to minimum
            if net_inventory > 0:
                bid_yes_size = self.p.min_maker_size
                bid_no_size = base_size
            elif net_inventory < 0:
                bid_yes_size = base_size
                bid_no_size = self.p.min_maker_size
            else:
                bid_yes_size = base_size
                bid_no_size = base_size
        else:
            bid_yes_size = base_size
            bid_no_size = base_size

        # --- Step 12: Trend-based side blocking ---
        # Strong trend: block the counter-trend entry side entirely
        # (unless we have inventory to exit on that side)
        if trend == TREND_STRONG_UP:
            # Don't open new NO positions (don't bet against the rally)
            if net_inventory >= 0:
                # No NO inventory to exit → block NO side
                bid_no_size = 0
            # But if we're already long NO, keep bid_no to exit
        elif trend == TREND_STRONG_DOWN:
            # Don't open new YES positions (don't buy into dump)
            if net_inventory <= 0:
                # No YES inventory to exit → block YES side
                bid_yes_size = 0

        # Trend + inventory combination: if trending one way and inv is
        # already building on that side, reduce entry to avoid over-concentration
        if trend in (TREND_STRONG_UP, TREND_MILD_UP):
            # Trending up, already long YES → reduce YES entry
            if net_inventory > self.s5.trend_block_inv:
                bid_yes_size = 0  # stop adding, let it run
        elif trend in (TREND_STRONG_DOWN, TREND_MILD_DOWN):
            # Trending down, already long NO → reduce NO entry
            if net_inventory < -self.s5.trend_block_inv:
                bid_no_size = 0

        # Enforce minimum maker size (0 is ok = don't quote)
        if 0 < bid_yes_size < self.p.min_maker_size:
            bid_yes_size = self.p.min_maker_size
        if 0 < bid_no_size < self.p.min_maker_size:
            bid_no_size = self.p.min_maker_size

        return QuotePair(
            bid_yes_price=bid_yes_price,
            bid_yes_size=bid_yes_size,
            bid_no_price=bid_no_price,
            bid_no_size=bid_no_size,
            reservation_price=round(reservation, 4),
            spread=round(spread, 4),
            sigma=round(sigma, 6),
        )
