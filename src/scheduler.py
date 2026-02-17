"""
Scheduler: hold current market, schedule switch at endDate, drive WebSocket resubscribe.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from . import clob_ws
from .gamma import MarketInfo, get_current_and_next_btc_5m
from .storage import OrderbookStorage, save_market_meta

logger = logging.getLogger(__name__)

RETRY_NO_MARKET_SEC = 30


class Scheduler:
    """
    Fetches current BTC 5min market, saves meta, runs WebSocket for that market;
    at endDate fetches next market, stops WS, restarts with new asset_ids.
    """

    def __init__(
        self,
        storage: OrderbookStorage,
        data_dir: str,
        *,
        gamma_base: str = "https://gamma-api.polymarket.com",
        ws_url: str = clob_ws.WS_URL,
    ):
        self.storage = storage
        self.data_dir = data_dir
        self.gamma_base = gamma_base
        self.ws_url = ws_url
        self._current: MarketInfo | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_stop = asyncio.Event()

    def get_current_slug(self) -> str:
        if self._current is None:
            return "unknown"
        return self._current.slug

    def _make_on_book(self):
        def on_book(payload: dict[str, Any]) -> None:
            slug = self.get_current_slug()
            ts_raw = payload.get("timestamp") or 0
            ts_ms = int(ts_raw) if ts_raw else 0
            self.storage.enqueue_snapshot(
                slug,
                ts_ms,
                str(payload.get("asset_id") or ""),
                str(payload.get("market") or ""),
                list(payload.get("bids") or []),
                list(payload.get("asks") or []),
            )

        return on_book

    def _make_on_price_change(self):
        def on_price_change(payload: dict[str, Any]) -> None:
            slug = self.get_current_slug()
            ts_raw = payload.get("timestamp") or 0
            ts_ms = int(ts_raw) if ts_raw else 0
            market = str(payload.get("market") or "")
            for pc in payload.get("price_changes") or []:
                if isinstance(pc, dict):
                    self.storage.enqueue_tick(
                        slug,
                        ts_ms,
                        str(pc.get("asset_id") or ""),
                        market,
                        str(pc.get("price") or ""),
                        str(pc.get("size") or ""),
                        str(pc.get("side") or ""),
                        str(pc.get("best_bid") or ""),
                        str(pc.get("best_ask") or ""),
                    )

        return on_price_change

    def _make_on_trade(self):
        def on_trade(payload: dict[str, Any]) -> None:
            slug = self.get_current_slug()
            ts_raw = payload.get("timestamp") or 0
            ts_ms = int(ts_raw) if ts_raw else 0
            self.storage.enqueue_trade(
                slug,
                ts_ms,
                str(payload.get("asset_id") or ""),
                str(payload.get("market") or ""),
                str(payload.get("price") or ""),
                str(payload.get("side") or ""),
                str(payload.get("size") or ""),
            )

        return on_trade

    async def _run_ws_for_market(self, market: MarketInfo) -> None:
        self._ws_stop.clear()
        await clob_ws.run_ws(
            market.asset_ids,
            on_book=self._make_on_book(),
            on_price_change=self._make_on_price_change(),
            on_trade=self._make_on_trade(),
            url=self.ws_url,
            stop_event=self._ws_stop,
        )

    def _switch_to(self, market: MarketInfo) -> None:
        self._current = market
        save_market_meta(
            self.data_dir,
            market.slug,
            condition_id=market.condition_id,
            end_date_utc_iso=market.end_date_utc.isoformat(),
            end_date_ts_ms=market.end_date_ts_ms,
            token_ids=market.token_ids,
            outcomes=market.outcomes,
        )
        logger.info(
            "Switched to market slug=%s endDate=%s token_ids=%s",
            market.slug,
            market.end_date_utc.isoformat(),
            market.token_ids,
        )

    async def _stop_ws(self) -> None:
        self._ws_stop.set()
        if self._ws_task is not None:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None

    async def run(self) -> None:
        """Main loop: fetch current market -> run WS until endDate -> repeat."""
        while True:
            try:
                current, _next = await get_current_and_next_btc_5m(base_url=self.gamma_base)
            except (httpx.ConnectError, httpx.TimeoutException, OSError) as e:
                logger.warning("Gamma API unreachable: %s; retry in %ss", e, RETRY_NO_MARKET_SEC)
                await asyncio.sleep(RETRY_NO_MARKET_SEC)
                continue
            if current is None:
                logger.warning("No current BTC 5m market; retry in %ss", RETRY_NO_MARKET_SEC)
                await asyncio.sleep(RETRY_NO_MARKET_SEC)
                continue

            self._switch_to(current)
            self._ws_stop.clear()
            self._ws_task = asyncio.create_task(self._run_ws_for_market(current))

            now = datetime.now(timezone.utc)
            end = current.end_date_utc
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
            delta = (end - now).total_seconds()
            if delta <= 0:
                delta = 1
            logger.info("Sleeping %.0fs until endDate %s", delta, end.isoformat())
            await asyncio.sleep(delta)

            await self._stop_ws()
