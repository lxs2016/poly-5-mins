# Polymarket BTC 5min Orderbook Monitor

Real-time orderbook capture for Polymarket’s **Bitcoin Up or Down - 5 min** markets via CLOB WebSocket. Data is stored by type for **real-time** and **backtest** arbitrage analysis.

## Requirements

- Python 3.10+
- Dependencies: `pip install -r requirements.txt`

## Config

- **config.yaml**: `data_dir`, `log_level`, `gamma_base_url`, `clob_ws_url`, `storage_flush_interval`, `storage_flush_max_rows`
- Env overrides: `DATA_DIR`, `LOG_LEVEL`, `GAMMA_BASE_URL`, `CLOB_WS_URL`

## Run

From project root:

```bash
python run.py
```

Flow:

1. Resolve current BTC 5m market from Gamma API (slug, `endDate`, token IDs).
2. Save market metadata under `data/markets/meta_{slug}.json`.
3. Connect to CLOB WebSocket, subscribe to the market’s two token IDs (Up/Down).
4. Append **book** snapshots and **price_change** ticks (and optional **last_trade_price**) to queues; a writer task batches and writes Parquet under `data/orderbook/` and `data/trades/`.
5. At market **endDate**, fetch the next 5m market, reconnect WebSocket with new token IDs, and repeat.

## Data layout (under `data_dir`)

| Path | Content |
|------|--------|
| `markets/meta_{slug}.json` | Event slug, condition_id, end_date_utc_iso, end_date_ts_ms, token_ids, outcomes |
| `orderbook/snapshots/{slug}/part_*.parquet` | Full book: ts_ms, asset_id, market, bids, asks |
| `orderbook/ticks/{slug}/part_*.parquet` | Price changes: ts_ms, asset_id, market, price, size, side, best_bid, best_ask |
| `trades/{slug}/part_*.parquet` | Last trade: ts_ms, asset_id, market, price, side, size |

All timestamps are in milliseconds (UTC). Use `slug` and `ts_ms` to join and align with market metadata.

## Backtest example (pandas)

Load orderbook snapshots and ticks for a given slug and time range:

```python
import pandas as pd
from pathlib import Path

data_dir = Path("data")
slug = "btc-updown-5m-1771342500"

# Snapshots (full book per event)
snap_dir = data_dir / "orderbook" / "snapshots" / slug
if snap_dir.exists():
    snap_df = pd.read_parquet(snap_dir)
    # Columns: slug, ts_ms, asset_id, market, bids, asks
    snap_df["ts"] = pd.to_datetime(snap_df["ts_ms"], unit="ms", utc=True)

# Ticks (price changes)
tick_dir = data_dir / "orderbook" / "ticks" / slug
if tick_dir.exists():
    tick_df = pd.read_parquet(tick_dir)
    # Columns: slug, ts_ms, asset_id, market, price, size, side, best_bid, best_ask
    tick_df["ts"] = pd.to_datetime(tick_df["ts_ms"], unit="ms", utc=True)

# Market meta
meta_path = data_dir / "markets" / f"meta_{slug}.json"
# Load with json for token_ids, end_date_utc_iso, etc.
```

Filter by time and join with `meta_{slug}.json` for token_id ↔ outcome mapping and resolution time.

## References

- [Polymarket CLOB - Get order book summary](https://docs.polymarket.com/api-reference/orderbook/get-order-book-summary)
- [Gamma API - Get Events](https://docs.polymarket.com/developers/gamma-markets-api/get-events)
- [CLOB WebSocket - Market Channel](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
