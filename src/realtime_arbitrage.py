"""
Real-time arbitrage detection: update best bid/ask from book callbacks,
detect buy_both/sell_both opportunities, and push to Discord with rate limiting.
Designed for minimal work in the WebSocket callback (no I/O, no pandas).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import httpx

from .gamma import MarketInfo

logger = logging.getLogger(__name__)

_SENTINEL: Any = object()  # stop signal for sender queue


def _price_from_level(level: Any) -> float:
    if isinstance(level, dict):
        return float(level.get("price", 0) or 0)
    if hasattr(level, "__len__") and len(level) >= 1:
        return float(level[0])
    return 0.0


def _best_bid_ask(bids: list, asks: list) -> tuple[float, float]:
    best_bid = 0.0
    best_ask = 1.0
    try:
        if bids:
            prices = [_price_from_level(x) for x in bids]
            best_bid = max(prices)
    except (TypeError, ValueError):
        pass
    try:
        if asks:
            prices = [_price_from_level(x) for x in asks]
            best_ask = min(prices)
    except (TypeError, ValueError):
        pass
    return best_bid, best_ask


@dataclass
class _ArbitrageSignal:
    slug: str
    ts_ms: int
    sum_ask: float
    sum_bid: float
    buy_opportunity: bool
    sell_opportunity: bool


class RealtimeArbitrage:
    """
    Holds per-market book state (best_bid/best_ask per asset), detects
    buy_both/sell_both opportunities on each book update, and enqueues
    signals for a background sender task that posts to Discord with rate limiting.
    """

    def __init__(
        self,
        webhook_url: str,
        *,
        buy_threshold: float = 0.99,
        sell_threshold: float = 1.01,
        min_interval_sec: float = 30,
    ):
        self._webhook_url = (webhook_url or "").strip()
        self._buy_threshold = buy_threshold
        self._sell_threshold = sell_threshold
        self._min_interval_sec = min_interval_sec
        self._book_state: dict[str, tuple[float, float]] = {}
        self._asset_to_outcome: dict[str, str] = {}
        self._current_slug: Optional[str] = None
        self._signal_q: asyncio.Queue[Any] = asyncio.Queue()
        self._sender_task: Optional[asyncio.Task[None]] = None
        self._stop = asyncio.Event()
        self._last_sent: dict[tuple[str, str], float] = {}

    def set_market(self, market: MarketInfo) -> None:
        """Reset state and set asset->outcome mapping for the new market."""
        self._current_slug = market.slug
        self._book_state.clear()
        token_ids = market.token_ids or []
        outcomes = (market.outcomes or ["Up", "Down"])[: len(token_ids)]
        self._asset_to_outcome = dict(zip(token_ids, outcomes))

    def update_book(
        self,
        slug: str,
        ts_ms: int,
        asset_id: str,
        bids: list,
        asks: list,
    ) -> None:
        """
        Synchronous, no I/O. Update best_bid/best_ask for this asset;
        if both assets have state and thresholds are met, enqueue a signal.
        """
        if not asset_id or slug != self._current_slug:
            return
        if asset_id not in self._asset_to_outcome:
            return
        best_bid, best_ask = _best_bid_ask(bids or [], asks or [])
        self._book_state[asset_id] = (best_bid, best_ask)
        if len(self._book_state) != 2:
            return
        # Both assets have state: get best_bid/best_ask by outcome
        ask_up = ask_down = 1.0
        bid_up = bid_down = 0.0
        for aid, (bid, ask) in self._book_state.items():
            if self._asset_to_outcome.get(aid) == "Up":
                bid_up, ask_up = bid, ask
            else:
                bid_down, ask_down = bid, ask
        sum_ask = ask_up + ask_down
        sum_bid = bid_up + bid_down
        buy_opportunity = sum_ask < self._buy_threshold
        sell_opportunity = sum_bid > self._sell_threshold
        if not (buy_opportunity or sell_opportunity):
            return
        if not self._webhook_url:
            return
        try:
            self._signal_q.put_nowait(
                _ArbitrageSignal(
                    slug=slug,
                    ts_ms=ts_ms,
                    sum_ask=sum_ask,
                    sum_bid=sum_bid,
                    buy_opportunity=buy_opportunity,
                    sell_opportunity=sell_opportunity,
                )
            )
        except asyncio.QueueFull:
            pass

    def _should_send(self, slug: str, buy_opportunity: bool, sell_opportunity: bool) -> bool:
        now = time.monotonic()
        key_buy = (slug, "buy")
        key_sell = (slug, "sell")
        if buy_opportunity:
            if now - self._last_sent.get(key_buy, 0) < self._min_interval_sec:
                return False
        if sell_opportunity:
            if now - self._last_sent.get(key_sell, 0) < self._min_interval_sec:
                return False
        return True

    def _mark_sent(self, slug: str, buy_opportunity: bool, sell_opportunity: bool) -> None:
        now = time.monotonic()
        if buy_opportunity:
            self._last_sent[(slug, "buy")] = now
        if sell_opportunity:
            self._last_sent[(slug, "sell")] = now

    async def _sender_loop(self) -> None:
        while not self._stop.is_set():
            try:
                signal = await asyncio.wait_for(
                    self._signal_q.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            if signal is _SENTINEL:
                break
            if not isinstance(signal, _ArbitrageSignal):
                continue
            if not self._should_send(
                signal.slug, signal.buy_opportunity, signal.sell_opportunity
            ):
                continue
            try:
                await self._post_discord(signal)
                self._mark_sent(
                    signal.slug, signal.buy_opportunity, signal.sell_opportunity
                )
            except Exception as e:
                logger.exception("Discord webhook send failed: %s", e)

    def _ts_fmt(self, ts_ms: int) -> str:
        try:
            return datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime(
                "%H:%M:%S"
            )
        except Exception:
            return str(ts_ms)

    async def _post_discord(self, signal: _ArbitrageSignal) -> None:
        kind = "买双" if signal.buy_opportunity else "卖双"
        desc = f"sum_ask={signal.sum_ask:.4f} sum_bid={signal.sum_bid:.4f}"
        body = {
            "embeds": [
                {
                    "title": "Polymarket 套利信号",
                    "description": "BTC 5min Up/Down 市场检测到套利机会。",
                    "color": 0x00FF00,
                    "fields": [
                        {"name": "类型", "value": kind, "inline": True},
                        {"name": "市场", "value": signal.slug, "inline": True},
                        {"name": "时间", "value": self._ts_fmt(signal.ts_ms), "inline": True},
                        {"name": "盘口", "value": desc, "inline": False},
                    ],
                }
            ],
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(self._webhook_url, json=body)
            r.raise_for_status()

    def start(self) -> None:
        if self._sender_task is not None:
            return
        self._stop.clear()
        self._sender_task = asyncio.create_task(self._sender_loop())
        logger.info("RealtimeArbitrage sender started")

    async def stop(self) -> None:
        self._stop.set()
        try:
            self._signal_q.put_nowait(_SENTINEL)
        except asyncio.QueueFull:
            pass
        if self._sender_task is not None:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
            self._sender_task = None
        logger.info("RealtimeArbitrage sender stopped")
