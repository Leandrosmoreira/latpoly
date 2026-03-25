"""Polymarket CLOB API client wrapper for live trading.

All sync py-clob-client calls are wrapped with asyncio.to_thread()
to avoid blocking the event loop.

Adapted from bookpoly/mmpoly reference implementation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.order_builder.constants import BUY, SELL

log = logging.getLogger(__name__)

CLOB_URL = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet

SIG_TYPE_EOA = 0
SIG_TYPE_POLY_PROXY = 1
SIG_TYPE_POLY_GNOSIS = 2


class PolyClient:
    """Async wrapper around py-clob-client for order management.

    Handles:
    - Connection with multiple credential formats
    - Limit BUY/SELL order placement (GTC)
    - Order cancellation with fill detection
    - Token approval for SELL orders
    """

    def __init__(self) -> None:
        self._client: Optional[ClobClient] = None
        self._approved_tokens: set[str] = set()
        self._funder: str = ""

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Initialize CLOB client from environment variables.

        Accepts POLYMARKET_* env vars (standard .env format).
        """
        def _env(*keys: str) -> str:
            for k in keys:
                v = os.environ.get(k, "").strip()
                if v:
                    return v
            return ""

        private_key = _env("POLYMARKET_PRIVATE_KEY", "POLY_PRIVATE_KEY")
        api_key = _env("POLYMARKET_API_KEY", "POLY_API_KEY")
        api_secret = _env("POLYMARKET_API_SECRET", "POLY_API_SECRET")
        passphrase = _env("POLYMARKET_PASSPHRASE", "POLY_API_PASSPHRASE")
        self._funder = _env("POLYMARKET_FUNDER", "POLY_FUNDER")

        # Determine signature type
        raw_sig = _env("POLYMARKET_SIGNATURE_TYPE", "POLY_WALLET_TYPE") or "0"
        sig_type = int(raw_sig) if raw_sig.isdigit() else SIG_TYPE_EOA

        log.info(
            "PolyClient connecting: has_key=%s has_api_key=%s sig_type=%d funder=%s",
            bool(private_key), bool(api_key), sig_type,
            (self._funder[:10] + "...") if self._funder else "none",
        )

        if not private_key:
            raise RuntimeError("No POLYMARKET_PRIVATE_KEY in environment")

        creds = None
        if api_key:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=passphrase,
            )

        self._client = ClobClient(
            CLOB_URL,
            key=private_key,
            chain_id=CHAIN_ID,
            signature_type=sig_type,
            funder=self._funder if self._funder else None,
            creds=creds,
        )

        # Derive API creds if not provided
        if not api_key:
            try:
                creds_raw = self._client.derive_api_key()
                if isinstance(creds_raw, dict):
                    creds = ApiCreds(
                        api_key=creds_raw.get("apiKey", creds_raw.get("api_key", "")),
                        api_secret=creds_raw.get("secret", creds_raw.get("api_secret", "")),
                        api_passphrase=creds_raw.get("passphrase", creds_raw.get("api_passphrase", "")),
                    )
                else:
                    creds = creds_raw
                self._client.set_api_creds(creds)
                log.info("API creds derived successfully")
            except Exception:
                log.exception("Failed to derive API credentials")
                raise

        # Health check
        self._client.get_ok()
        log.info("PolyClient connected (sig_type=%d)", sig_type)

    @property
    def connected(self) -> bool:
        return self._client is not None

    # ------------------------------------------------------------------
    # Token balance check
    # ------------------------------------------------------------------

    async def get_token_balance(self, token_id: str) -> int:
        """Check how many conditional token SHARES are in the wallet.

        Returns number of shares (not raw units).
        Polymarket uses 6 decimals: 1 share = 1,000,000 raw units.
        """
        if not self._client:
            return 0
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            result = await asyncio.to_thread(
                self._client.get_balance_allowance, params
            )
            if isinstance(result, dict):
                raw = result.get("balance", 0)
                raw_int = int(float(str(raw))) if raw else 0
                # Convert from raw units (6 decimals) to shares
                shares = raw_int // 1_000_000
                log.debug(
                    "Token balance for %s...: %d shares (raw=%d)",
                    token_id[:16], shares, raw_int,
                )
                return shares
            return 0
        except Exception as e:
            log.debug("get_token_balance failed for %s...: %s", token_id[:16], e)
            return 0

    # ------------------------------------------------------------------
    # Token approval (required before SELL)
    # ------------------------------------------------------------------

    async def approve_token(self, token_id: str) -> bool:
        """Approve a conditional token for trading (allowance).

        Required once per token before any SELL order.
        """
        if token_id in self._approved_tokens:
            return True
        if not self._client:
            return False

        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            await asyncio.to_thread(
                self._client.update_balance_allowance, params
            )
            self._approved_tokens.add(token_id)
            log.info("Token approved: %s...", token_id[:16])
            return True
        except Exception as e:
            log.error("Token approval failed for %s...: %s", token_id[:16], e)
            return False

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def place_limit_buy(
        self, token_id: str, price: float, size: int,
    ) -> Optional[str]:
        """Place GTC limit BUY order. Returns order_id or None."""
        if not self._client:
            return None

        try:
            args = OrderArgs(
                price=price,
                size=size,
                side=BUY,
                token_id=token_id,
            )
            signed = await asyncio.to_thread(self._client.create_order, args)
            resp = await asyncio.to_thread(
                self._client.post_order, signed, OrderType.GTC
            )

            if resp and resp.get("success"):
                oid = resp.get("orderID", resp.get("order_id", ""))
                log.info(
                    "BUY placed: %s @ $%.2f sz=%d token=%s...",
                    oid[:16], price, size, token_id[:16],
                )
                return oid

            log.warning("BUY rejected: %s", resp)
            return None

        except Exception as e:
            err = str(e).lower()
            if "not enough balance" in err:
                log.error(
                    "Insufficient USDC: BUY px=%.2f sz=%d cost=$%.2f",
                    price, size, price * size,
                )
            else:
                log.error("BUY failed: %s", e)
            return None

    async def place_limit_sell(
        self, token_id: str, price: float, size: int,
    ) -> Optional[str]:
        """Place GTC limit SELL order. Auto-approves token if needed."""
        if not self._client:
            return None

        # Approve token before first SELL
        await self.approve_token(token_id)

        try:
            args = OrderArgs(
                price=price,
                size=size,
                side=SELL,
                token_id=token_id,
            )
            signed = await asyncio.to_thread(self._client.create_order, args)
            resp = await asyncio.to_thread(
                self._client.post_order, signed, OrderType.GTC
            )

            if resp and resp.get("success"):
                oid = resp.get("orderID", resp.get("order_id", ""))
                log.info(
                    "SELL placed: %s @ $%.2f sz=%d token=%s...",
                    oid[:16], price, size, token_id[:16],
                )
                return oid

            log.warning("SELL rejected: %s", resp)
            return None

        except Exception as e:
            err = str(e).lower()
            log.error(
                "SELL exception: px=%.2f sz=%d token=%s... error=%s",
                price, size, token_id[:16], e,
            )
            if "not enough balance" in err or "allowance" in err:
                # Retry with fresh token approval
                log.info("SELL failed (balance/allowance), retrying with fresh approval...")
                self._approved_tokens.discard(token_id)
                if await self.approve_token(token_id):
                    return await self._place_limit_sell_inner(
                        token_id, price, size
                    )
                log.error("SELL failed after re-approval: %s", e)
            return None

    async def _place_limit_sell_inner(
        self, token_id: str, price: float, size: int,
    ) -> Optional[str]:
        """Internal SELL without re-approval (avoid infinite recursion)."""
        if not self._client:
            return None
        try:
            args = OrderArgs(price=price, size=size, side=SELL, token_id=token_id)
            signed = await asyncio.to_thread(self._client.create_order, args)
            resp = await asyncio.to_thread(
                self._client.post_order, signed, OrderType.GTC
            )
            if resp and resp.get("success"):
                return resp.get("orderID", resp.get("order_id", ""))
            return None
        except Exception as e:
            log.error("SELL retry failed: px=%.2f sz=%d error=%s", price, size, e)
            return None

    async def place_market_sell(
        self, token_id: str, price: float, size: int,
    ) -> Optional[str]:
        """Place FOK (Fill-Or-Kill) taker SELL — executes immediately at market."""
        if not self._client:
            return None

        await self.approve_token(token_id)

        try:
            args = OrderArgs(
                price=price,
                size=size,
                side=SELL,
                token_id=token_id,
            )
            signed = await asyncio.to_thread(self._client.create_order, args)
            resp = await asyncio.to_thread(
                self._client.post_order, signed, OrderType.FOK
            )

            if resp and resp.get("success"):
                oid = resp.get("orderID", resp.get("order_id", ""))
                log.info(
                    "SELL TAKER (FOK) placed: %s @ $%.2f sz=%d token=%s...",
                    oid[:16], price, size, token_id[:16],
                )
                return oid

            log.warning("SELL TAKER rejected: %s", resp)
            return None

        except Exception as e:
            log.error(
                "SELL TAKER exception: px=%.2f sz=%d token=%s... error=%s",
                price, size, token_id[:16], e,
            )
            return None

    # ------------------------------------------------------------------
    # Order cancellation
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> str:
        """Cancel a specific order.

        Returns:
            "canceled" — successfully canceled (entry never filled)
            "matched" — order was already filled (infer fill)
            "gone"    — already canceled or not found
            "failed"  — actual failure
        """
        if not self._client:
            return "failed"

        try:
            resp = await asyncio.to_thread(self._client.cancel, order_id)
            if isinstance(resp, dict):
                canceled_list = resp.get("canceled", [])
                not_canceled = resp.get("not_canceled", {})
                if order_id in canceled_list or resp.get("success"):
                    return "canceled"
                if not_canceled:
                    reason = str(not_canceled).lower()
                    if "matched" in reason:
                        return "matched"
                    if "already" in reason or "not found" in reason:
                        return "gone"
            elif resp:
                return "canceled"
            return "failed"

        except Exception as e:
            err = str(e).lower()
            if "matched" in err:
                return "matched"
            if "not found" in err or "already" in err:
                return "gone"
            log.error("Cancel %s failed: %s", order_id[:16], e)
            return "failed"

    async def get_order(self, order_id: str) -> Optional[dict]:
        """Fetch order details from the exchange.

        Returns raw order dict with fields like:
          size_matched, original_size, price, side, status, ...
        Returns None on failure.
        """
        if not self._client:
            return None
        try:
            resp = await asyncio.to_thread(self._client.get_order, order_id)
            if isinstance(resp, dict):
                return resp
            return None
        except Exception as e:
            log.debug("get_order %s failed: %s", order_id[:16], e)
            return None

    async def get_filled_size(self, order_id: str) -> int:
        """Get how many shares were filled for an order.

        Returns 0 if order not found or no fills.
        Checks 'size_matched' field from Polymarket API.
        """
        order = await self.get_order(order_id)
        if order is None:
            return 0
        # Polymarket API returns size_matched as string or number
        matched = order.get("size_matched") or order.get("matched_size") or 0
        try:
            return int(float(str(matched)))
        except (ValueError, TypeError):
            return 0

    async def cancel_all(self) -> bool:
        """Cancel all open orders on the exchange."""
        if not self._client:
            return False

        try:
            await asyncio.to_thread(self._client.cancel_all)
            log.info("All orders cancelled")
            return True
        except Exception as e:
            log.error("Cancel all failed: %s", e)
            return False
