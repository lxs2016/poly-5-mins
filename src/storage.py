"""
Storage layer: market metadata (JSON) and orderbook snapshots/ticks/trades (Parquet via queue + batch write).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 5.0
FLUSH_MAX_ROWS = 200


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_market_meta(
    data_dir: str | Path,
    slug: str,
    *,
    condition_id: str = "",
    end_date_utc_iso: str = "",
    end_date_ts_ms: int = 0,
    token_ids: list[str] | None = None,
    outcomes: list[str] | None = None,
) -> None:
    """Write one market metadata JSON to data_dir/markets/meta_{slug}.json."""
    root = Path(data_dir)
    _ensure_dir(root / "markets")
    path = root / "markets" / f"meta_{slug}.json"
    payload = {
        "slug": slug,
        "condition_id": condition_id,
        "end_date_utc_iso": end_date_utc_iso,
        "end_date_ts_ms": end_date_ts_ms,
        "token_ids": token_ids or [],
        "outcomes": outcomes or ["Up", "Down"],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    logger.info("Saved market meta: %s", path)


# --- Queued writer for snapshots / ticks / trades ---

def _snapshot_row(slug: str, ts_ms: int, asset_id: str, market: str, bids: list, asks: list) -> dict[str, Any]:
    return {
        "slug": slug,
        "ts_ms": ts_ms,
        "asset_id": asset_id,
        "market": market,
        "bids": bids,
        "asks": asks,
    }


def _tick_row(
    slug: str,
    ts_ms: int,
    asset_id: str,
    market: str,
    price: str,
    size: str,
    side: str,
    best_bid: str,
    best_ask: str,
) -> dict[str, Any]:
    return {
        "slug": slug,
        "ts_ms": ts_ms,
        "asset_id": asset_id,
        "market": market,
        "price": price,
        "size": size,
        "side": side,
        "best_bid": best_bid,
        "best_ask": best_ask,
    }


def _trade_row(slug: str, ts_ms: int, asset_id: str, market: str, price: str, side: str, size: str) -> dict[str, Any]:
    return {
        "slug": slug,
        "ts_ms": ts_ms,
        "asset_id": asset_id,
        "market": market,
        "price": price,
        "side": side,
        "size": size,
    }


class OrderbookStorage:
    """
    Holds queues and a background writer task. Call enqueue_* from WebSocket callbacks;
    writer batches and flushes to Parquet by slug (part files under slug dir).
    """

    def __init__(
        self,
        data_dir: str | Path,
        *,
        flush_interval: float = FLUSH_INTERVAL,
        flush_max_rows: int = FLUSH_MAX_ROWS,
    ):
        self.data_dir = Path(data_dir)
        self.flush_interval = flush_interval
        self.flush_max_rows = flush_max_rows
        self._snapshot_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._tick_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._trade_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._writer_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def enqueue_snapshot(self, slug: str, ts_ms: int, asset_id: str, market: str, bids: list, asks: list) -> None:
        self._snapshot_q.put_nowait(_snapshot_row(slug, ts_ms, asset_id, market, bids, asks))

    def enqueue_tick(
        self,
        slug: str,
        ts_ms: int,
        asset_id: str,
        market: str,
        price: str,
        size: str,
        side: str,
        best_bid: str,
        best_ask: str,
    ) -> None:
        self._tick_q.put_nowait(
            _tick_row(slug, ts_ms, asset_id, market, price, size, side, best_bid, best_ask)
        )

    def enqueue_trade(self, slug: str, ts_ms: int, asset_id: str, market: str, price: str, side: str, size: str) -> None:
        self._trade_q.put_nowait(_trade_row(slug, ts_ms, asset_id, market, price, side, size))

    @staticmethod
    def _write_parquet_part(path: Path, df: pd.DataFrame) -> None:
        if df.empty:
            return
        _ensure_dir(path.parent)
        df.to_parquet(path, index=False)

    async def _flush_buffers(
        self,
        snap_buf: dict[str, list[dict]],
        tick_buf: dict[str, list[dict]],
        trade_buf: dict[str, list[dict]],
    ) -> None:
        now_ms = int(asyncio.get_event_loop().time() * 1000)
        base = self.data_dir
        for slug, rows in snap_buf.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            path = base / "orderbook" / "snapshots" / slug / f"part_{now_ms}.parquet"
            self._write_parquet_part(path, df)
            snap_buf[slug] = []
        for slug, rows in tick_buf.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            path = base / "orderbook" / "ticks" / slug / f"part_{now_ms}.parquet"
            self._write_parquet_part(path, df)
            tick_buf[slug] = []
        for slug, rows in trade_buf.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            path = base / "trades" / slug / f"part_{now_ms}.parquet"
            self._write_parquet_part(path, df)
            trade_buf[slug] = []

    async def _writer_loop(self) -> None:
        snap_buf: dict[str, list[dict]] = defaultdict(list)
        tick_buf: dict[str, list[dict]] = defaultdict(list)
        trade_buf: dict[str, list[dict]] = defaultdict(list)
        last_flush = asyncio.get_event_loop().time()

        while not self._stop.is_set():
            for q, buf, key in [
                (self._snapshot_q, snap_buf, "slug"),
                (self._tick_q, tick_buf, "slug"),
                (self._trade_q, trade_buf, "slug"),
            ]:
                while not q.empty():
                    try:
                        row = q.get_nowait()
                        slug = row.get(key, "unknown")
                        buf[slug].append(row)
                    except asyncio.QueueEmpty:
                        break

            now = asyncio.get_event_loop().time()
            total = sum(len(v) for v in snap_buf.values()) + sum(len(v) for v in tick_buf.values()) + sum(len(v) for v in trade_buf.values())
            if now - last_flush >= self.flush_interval or total >= self.flush_max_rows:
                await self._flush_buffers(snap_buf, tick_buf, trade_buf)
                last_flush = now

            await asyncio.sleep(0.1)

        # Final flush
        await self._flush_buffers(snap_buf, tick_buf, trade_buf)

    def start_writer(self) -> None:
        if self._writer_task is not None:
            return
        self._stop.clear()
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info("Storage writer started")

    async def stop_writer(self) -> None:
        self._stop.set()
        if self._writer_task is not None:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None
        logger.info("Storage writer stopped")
