"""
CLOB WebSocket client: subscribe to market channel for orderbook (book, price_change, last_trade_price).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Awaitable

import websockets
from websockets.asyncio.client import ClientConnection

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10

logger = logging.getLogger(__name__)


# Callback types: (payload) -> None or await callback(payload)
OnBook = Callable[[dict[str, Any]], Awaitable[None] | None]
OnPriceChange = Callable[[dict[str, Any]], Awaitable[None] | None]
OnTrade = Callable[[dict[str, Any]], Awaitable[None] | None]


async def _run_callback(cb: OnBook | OnPriceChange | OnTrade | None, payload: dict[str, Any]) -> None:
    if cb is None:
        return
    try:
        result = cb(payload)
        if asyncio.iscoroutine(result):
            await result
    except Exception as e:
        logger.exception("Callback error: %s", e)


def _parse_book(msg: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize book message: ensure bids/asks (doc sometimes says buys/sells)."""
    event = (msg.get("event_type") or msg.get("eventType") or "").lower()
    if event != "book":
        return None
    bids = msg.get("bids") or msg.get("buys") or []
    asks = msg.get("asks") or msg.get("sells") or []
    return {
        "event_type": "book",
        "asset_id": msg.get("asset_id"),
        "market": msg.get("market"),
        "timestamp": msg.get("timestamp"),
        "hash": msg.get("hash"),
        "bids": list(bids),
        "asks": list(asks),
    }


def _parse_price_change(msg: dict[str, Any]) -> dict[str, Any] | None:
    event = (msg.get("event_type") or msg.get("eventType") or "").lower()
    if "price_change" not in event:
        return None
    changes = msg.get("price_changes") or msg.get("priceChanges") or []
    return {
        "event_type": "price_change",
        "market": msg.get("market"),
        "timestamp": msg.get("timestamp"),
        "price_changes": list(changes),
    }


def _parse_last_trade(msg: dict[str, Any]) -> dict[str, Any] | None:
    event = (msg.get("event_type") or msg.get("eventType") or "").lower()
    if "last_trade" not in event:
        return None
    return {
        "event_type": "last_trade_price",
        "asset_id": msg.get("asset_id"),
        "market": msg.get("market"),
        "timestamp": msg.get("timestamp"),
        "price": msg.get("price"),
        "side": msg.get("side"),
        "size": msg.get("size"),
        "fee_rate_bps": msg.get("fee_rate_bps"),
    }


async def run_ws(
    asset_ids: list[str],
    *,
    on_book: OnBook | None = None,
    on_price_change: OnPriceChange | None = None,
    on_trade: OnTrade | None = None,
    url: str = WS_URL,
    ping_interval: float = PING_INTERVAL,
    stop_event: asyncio.Event | None = None,
) -> None:
    """
    Connect to CLOB market WebSocket, subscribe to asset_ids, and dispatch messages.
    Runs until the connection is closed or stop_event is set.
    Sends PING every ping_interval seconds to keep connection alive.
    """
    if not asset_ids:
        logger.warning("run_ws called with empty asset_ids")
        return
    subscribe_msg = json.dumps({"assets_ids": asset_ids, "type": "market"})

    async def ping_loop(ws: ClientConnection) -> None:
        try:
            while True:
                await asyncio.sleep(ping_interval)
                if stop_event and stop_event.is_set():
                    return
                await ws.send("PING")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("ping_loop exit: %s", e)

    async def reader(ws: ClientConnection) -> None:
        try:
            async for raw in ws:
                if stop_event and stop_event.is_set():
                    return
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                s = (raw or "").strip()
                if s in ("PONG", "pong") or s.startswith("{"):
                    pass
                else:
                    continue
                if not s.startswith("{"):
                    continue
                try:
                    msg = json.loads(s)
                except json.JSONDecodeError:
                    continue
                event = (msg.get("event_type") or msg.get("eventType") or "").lower()
                if "book" in event:
                    parsed = _parse_book(msg)
                    if parsed:
                        await _run_callback(on_book, parsed)
                elif "price_change" in event:
                    parsed = _parse_price_change(msg)
                    if parsed:
                        await _run_callback(on_price_change, parsed)
                elif "last_trade" in event:
                    parsed = _parse_last_trade(msg)
                    if parsed:
                        await _run_callback(on_trade, parsed)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("reader error: %s", e)

    while True:
        if stop_event and stop_event.is_set():
            break
        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=15,
                open_timeout=45,
            ) as ws:
                await ws.send(subscribe_msg)
                logger.info("Subscribed to %s assets at %s", len(asset_ids), url)
                ping_task = asyncio.create_task(ping_loop(ws))
                try:
                    await reader(ws)
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            err_msg = str(e).strip() or repr(e)
            logger.warning(
                "WebSocket connection error: %s (%s); reconnecting in 5s",
                type(e).__name__,
                err_msg,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            await asyncio.sleep(5)
        if stop_event and stop_event.is_set():
            break
