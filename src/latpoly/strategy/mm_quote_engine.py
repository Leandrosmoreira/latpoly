"""MM Quote Engine — Avellaneda-Stoikov simplified for Polymarket.

Pure math module. No I/O, no async, no side effects.

Computes optimal bid/ask quotes based on:
  - Current mid price (from Polymarket book)
  - Net inventory (positive = long YES, negative = long NO)
  - Volatility (estimated from Binance short returns)
  - Time to expiry (normalized to cycle window)
  - Adverse selection risk (Binance move since last Polymarket update)

Config (env vars):
  LATPOLY_MM_GAMMA              = 0.5     # risk aversion
  LATPOLY_MM_BASE_SPREAD        = 4       # min spread in ticks
  LATPOLY_MM_MAX_SPREAD         = 12      # max spread in ticks
  LATPOLY_MM_MAX_INVENTORY      = 8       # hard limit
  LATPOLY_MM_SOFT_INVENTORY     = 4       # skew threshold
  LATPOLY_MM_QUOTE_SIZE         = 5       # shares per side
  LATPOLY_MM_MIN_MAKER_SIZE     = 5       # Polymarket minimum
  LATPOLY_MM_ADVERSE_THRESHOLD  = 10.0    # bn_move to widen
  LATPOLY_MM_ADVERSE_EXTRA_TICKS = 3      # extra spread on adverse
  LATPOLY_MM_NORMAL_CUTOFF_S    = 120
  LATPOLY_MM_REDUCE_CUTOFF_S    = 60
  LATPOLY_MM_EXIT_CUTOFF_S      = 30
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TICK_SIZE = 0.01
MIN_PRICE = 0.01
MAX_PRICE = 0.99
SIGMA_MIN = 0.0001
SIGMA_MAX = 0.05


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))

def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))

def _floor_tick(price: float) -> float:
    """Floor to nearest tick."""
    return math.floor(price / TICK_SIZE) * TICK_SIZE

def _ceil_tick(price: float) -> float:
    """Ceil to nearest tick."""
    return math.ceil(price / TICK_SIZE) * TICK_SIZE

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class QuotePair:
    """Desired quotes for one market slot."""
    bid_yes_price: float      # price to BUY YES tokens
    bid_yes_size: int         # 0 = don't quote this side
    bid_no_price: float       # price to BUY NO tokens (= ASK YES @ $1 - price)
    bid_no_size: int          # 0 = don't quote this side
    reservation_price: float  # mid adjusted by inventory (for logging)
    spread: float             # computed spread (for logging)
    sigma: float              # estimated volatility (for logging)


@dataclass
class MMParams:
    """Market maker parameters — all from env vars."""
    gamma: float = 0.0
    base_spread_ticks: int = 0
    max_spread_ticks: int = 0
    max_inventory: int = 0
    soft_inventory: int = 0
    quote_size: int = 0
    min_maker_size: int = 0
    adverse_threshold: float = 0.0
    adverse_extra_ticks: int = 0
    normal_cutoff_s: float = 0.0
    reduce_cutoff_s: float = 0.0
    exit_cutoff_s: float = 0.0
    max_data_age_ms: float = 0.0
    fill_check_interval_s: float = 0.0
    max_reprices_per_s: int = 0
    cycle_window_s: float = 0.0

    def __init__(self) -> None:
        self.gamma = _env_float("LATPOLY_MM_GAMMA", 0.5)
        self.base_spread_ticks = _env_int("LATPOLY_MM_BASE_SPREAD", 4)
        self.max_spread_ticks = _env_int("LATPOLY_MM_MAX_SPREAD", 12)
        self.max_inventory = _env_int("LATPOLY_MM_MAX_INVENTORY", 8)
        self.soft_inventory = _env_int("LATPOLY_MM_SOFT_INVENTORY", 4)
        self.quote_size = _env_int("LATPOLY_MM_QUOTE_SIZE", 5)
        self.min_maker_size = _env_int("LATPOLY_MM_MIN_MAKER_SIZE", 5)
        self.adverse_threshold = _env_float("LATPOLY_MM_ADVERSE_THRESHOLD", 10.0)
        self.adverse_extra_ticks = _env_int("LATPOLY_MM_ADVERSE_EXTRA_TICKS", 3)
        self.normal_cutoff_s = _env_float("LATPOLY_MM_NORMAL_CUTOFF_S", 120.0)
        self.reduce_cutoff_s = _env_float("LATPOLY_MM_REDUCE_CUTOFF_S", 60.0)
        self.exit_cutoff_s = _env_float("LATPOLY_MM_EXIT_CUTOFF_S", 30.0)
        self.max_data_age_ms = _env_float("LATPOLY_MM_MAX_DATA_AGE_MS", 5000.0)
        self.fill_check_interval_s = _env_float("LATPOLY_MM_FILL_CHECK_INTERVAL", 3.0)
        self.max_reprices_per_s = _env_int("LATPOLY_MM_MAX_REPRICES_PER_S", 1)
        self.cycle_window_s = _env_float("LATPOLY_MM_CYCLE_WINDOW_S", 900.0)


# ---------------------------------------------------------------------------
# Quote Engine
# ---------------------------------------------------------------------------


class MMQuoteEngine:
    """Avellaneda-Stoikov simplified quote calculator for Polymarket."""

    def __init__(self, params: MMParams) -> None:
        self.p = params

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_quotes(
        self,
        tick: dict,
        net_inventory: int,
        time_phase: str,
    ) -> QuotePair | None:
        """Compute optimal bid/ask quotes.

        Args:
            tick: normalized tick dict from signal worker
            net_inventory: positive = long YES, negative = long NO
            time_phase: "normal", "reduce", "exit", "halt"

        Returns:
            QuotePair with desired prices and sizes, or None if cannot quote.
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

        # --- Step 1: Estimate volatility ---
        sigma = self.estimate_sigma(tick)

        # --- Step 2: Normalized time ---
        T = (tte_ms / 1000.0) / self.p.cycle_window_s
        T = max(T, 0.001)  # avoid division by zero

        # --- Step 3: Reservation price (Avellaneda-Stoikov) ---
        # reservation = mid - q * gamma * sigma^2 * T
        q = net_inventory
        reservation = mid_yes - q * self.p.gamma * (sigma ** 2) * T

        # --- Step 4: Optimal spread ---
        # spread = gamma * sigma^2 * T + base_spread
        spread = self.p.gamma * (sigma ** 2) * T + self.p.base_spread_ticks * TICK_SIZE

        # --- Step 5: Adverse selection widening ---
        bn_move = tick.get("bn_move_since_poly")
        if bn_move is not None and abs(bn_move) > self.p.adverse_threshold:
            spread += self.p.adverse_extra_ticks * TICK_SIZE

        # --- Step 6: Time phase adjustments ---
        if time_phase == "reduce":
            spread *= 1.5  # wider spread in reduce phase
        elif time_phase == "exit":
            spread *= 2.0  # much wider in exit phase

        # Clamp spread
        spread = _clamp(
            spread,
            self.p.base_spread_ticks * TICK_SIZE,
            self.p.max_spread_ticks * TICK_SIZE,
        )

        # --- Step 7: Compute bid/ask for YES ---
        bid_yes_raw = reservation - spread / 2.0
        ask_yes_raw = reservation + spread / 2.0

        bid_yes_price = round(_floor_tick(bid_yes_raw), 2)
        ask_yes_price = round(_ceil_tick(ask_yes_raw), 2)

        # Ensure minimum 1 tick spread
        if ask_yes_price <= bid_yes_price:
            ask_yes_price = round(bid_yes_price + TICK_SIZE, 2)

        # --- Step 8: Convert to BID NO ---
        # BUY NO @ X  =  ASK YES @ (1 - X)
        # So bid_no_price = 1.00 - ask_yes_price
        bid_no_price = round(1.0 - ask_yes_price, 2)

        # Clamp all prices
        bid_yes_price = round(_clamp(bid_yes_price, MIN_PRICE, MAX_PRICE), 2)
        bid_no_price = round(_clamp(bid_no_price, MIN_PRICE, MAX_PRICE), 2)

        # --- Step 9: Compute sizes with inventory skew ---
        abs_inv = abs(net_inventory)
        base_size = self.p.quote_size

        if time_phase == "exit":
            # Exit mode: only quote the exit side
            if net_inventory > 0:
                # Long YES → only ask (= bid_no)
                bid_yes_size = 0
                bid_no_size = min(base_size, net_inventory)
            elif net_inventory < 0:
                # Long NO → only bid_yes to buy back
                bid_yes_size = min(base_size, abs_inv)
                bid_no_size = 0
            else:
                # Flat in exit mode → don't open new risk
                bid_yes_size = 0
                bid_no_size = 0
        elif abs_inv >= self.p.max_inventory:
            # Hard limit: cancel entry side entirely
            if net_inventory > 0:
                # Long YES → cancel bid_yes, keep bid_no (= ask_yes for exit)
                bid_yes_size = 0
                bid_no_size = base_size
            else:
                # Long NO → cancel bid_no, keep bid_yes for exit
                bid_yes_size = base_size
                bid_no_size = 0
        elif abs_inv >= self.p.soft_inventory:
            # Soft limit: reduce entry side to minimum
            if net_inventory > 0:
                bid_yes_size = self.p.min_maker_size  # reduced
                bid_no_size = base_size               # full (exit side)
            elif net_inventory < 0:
                bid_yes_size = base_size               # full (exit side)
                bid_no_size = self.p.min_maker_size   # reduced
            else:
                bid_yes_size = base_size
                bid_no_size = base_size
        else:
            # Normal: both sides full size
            bid_yes_size = base_size
            bid_no_size = base_size

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

    # ------------------------------------------------------------------
    # Volatility estimation
    # ------------------------------------------------------------------

    @staticmethod
    def estimate_sigma(tick: dict) -> float:
        """Estimate short-term volatility from Binance returns.

        Uses max(|ret_1s|, |ret_3s|/sqrt(3)) as instantaneous sigma proxy.
        This gives a fast-reacting estimate that widens spread when BTC moves.
        """
        ret_1s = tick.get("ret_1s")
        ret_3s = tick.get("ret_3s")

        candidates = []
        if ret_1s is not None:
            candidates.append(abs(ret_1s))
        if ret_3s is not None:
            candidates.append(abs(ret_3s) / math.sqrt(3))

        if not candidates:
            return SIGMA_MIN  # no data, use minimum

        sigma = max(candidates)
        return _clamp(sigma, SIGMA_MIN, SIGMA_MAX)

    # ------------------------------------------------------------------
    # Time phase
    # ------------------------------------------------------------------

    def compute_time_phase(self, tte_s: float) -> str:
        """Determine quoting phase from seconds to expiry.

        Returns: "normal", "reduce", "exit", "halt"
        """
        if tte_s < self.p.exit_cutoff_s:
            return "halt"
        if tte_s < self.p.reduce_cutoff_s:
            return "exit"
        if tte_s < self.p.normal_cutoff_s:
            return "reduce"
        return "normal"
