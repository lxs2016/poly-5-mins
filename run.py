#!/usr/bin/env python3
"""
Entry point: load config, start storage writer, run scheduler (Gamma + WebSocket + switch at endDate).
"""
from __future__ import annotations

import asyncio
import logging
import sys

from src.config import load_config
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
    scheduler = Scheduler(
        storage,
        data_dir,
        gamma_base=cfg.get("gamma_base_url", "https://gamma-api.polymarket.com"),
        ws_url=cfg.get("clob_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
    )
    try:
        await scheduler.run()
    finally:
        await storage.stop_writer()


if __name__ == "__main__":
    asyncio.run(main())
