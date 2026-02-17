#!/usr/bin/env python3
"""
Entry point: load config, start storage writer, run scheduler (Gamma + WebSocket + switch at endDate).
"""
from __future__ import annotations

import asyncio
import logging
import sys

from src.config import load_config
from src.realtime_arbitrage import RealtimeArbitrage
from src.scheduler import Scheduler
from src.storage import OrderbookStorage


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


async def main() -> None:
    cfg = load_config()
    setup_logging(cfg.get("log_level", "INFO"))
    logger = logging.getLogger("run")
    data_dir = cfg["data_dir"]
    storage = OrderbookStorage(
        data_dir,
        flush_interval=float(cfg.get("storage_flush_interval", 5)),
        flush_max_rows=int(cfg.get("storage_flush_max_rows", 200)),
    )
    storage.start_writer()

    realtime_arbitrage: RealtimeArbitrage | None = None
    discord_url = (cfg.get("discord_webhook_url") or "").strip()
    if discord_url:
        realtime_arbitrage = RealtimeArbitrage(
            discord_url,
            buy_threshold=float(cfg.get("arbitrage_buy_threshold", 0.99)),
            sell_threshold=float(cfg.get("arbitrage_sell_threshold", 1.01)),
            min_interval_sec=float(cfg.get("arbitrage_discord_min_interval_sec", 30)),
        )
        realtime_arbitrage.start()

    scheduler = Scheduler(
        storage,
        data_dir,
        gamma_base=cfg.get("gamma_base_url", "https://gamma-api.polymarket.com"),
        ws_url=cfg.get("clob_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        realtime_arbitrage=realtime_arbitrage,
    )
    try:
        await scheduler.run()
    finally:
        if realtime_arbitrage is not None:
            await realtime_arbitrage.stop()
        await storage.stop_writer()


if __name__ == "__main__":
    asyncio.run(main())
