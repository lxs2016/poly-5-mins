#!/usr/bin/env python3
"""
套利信号分析脚本：基于已落盘的 orderbook 快照与市场元数据，
计算 Up/Down 二元市场的 mid、spread、sum_ask/sum_bid，并检测套利机会。

二元市场理论：Up + Down = 1。
- 若 best_ask_Up + best_ask_Down < 1 - 阈值：买入两边可锁利（买双套利）。
- 若 best_bid_Up + best_bid_Down > 1 + 阈值：卖出两边可锁利（卖双套利）。

用法:
  python scripts/analyze_arbitrage.py [--data-dir DATA_DIR] [--slug SLUG] [--output CSV_PATH] [--threshold 0.01]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import httpx

# 兼容从项目根或 scripts 目录运行
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.config import load_config
except ImportError:
    load_config = None


def _ensure_data_dir(data_dir: Path) -> None:
    if not data_dir.is_dir():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")


def load_market_meta(data_dir: Path, slug: str) -> dict:
    """加载单个市场的元数据，返回 slug, token_ids, outcomes 等。"""
    path = data_dir / "markets" / f"meta_{slug}.json"
    if not path.exists():
        raise FileNotFoundError(f"Market meta not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def list_slugs_from_meta(data_dir: Path) -> list[str]:
    """从 data_dir/markets/meta_*.json 列出所有 btc-updown-5m 的 slug。"""
    meta_dir = data_dir / "markets"
    if not meta_dir.is_dir():
        return []
    slugs = []
    for p in meta_dir.glob("meta_btc-updown-5m-*.json"):
        # 排除 test
        name = p.stem.replace("meta_", "")
        if "test" in name.lower():
            continue
        slugs.append(name)
    return sorted(slugs)


def _price_from_level(level) -> float:
    if isinstance(level, dict):
        return float(level.get("price", 0) or 0)
    if hasattr(level, "__len__") and len(level) >= 1:
        return float(level[0])
    return 0.0


def _best_bid_ask(bids: list, asks: list) -> tuple[float, float]:
    """从 snapshot 的 bids/asks 列表取最佳买一/卖一。存储可能为 bids 升序、asks 降序，故取最高买价与最低卖价。"""
    best_bid = 0.0
    best_ask = 1.0
    try:
        bid_list = list(bids) if bids is not None else []
        if bid_list:
            prices = [_price_from_level(x) for x in bid_list]
            best_bid = max(prices)
    except (TypeError, ValueError):
        pass
    try:
        ask_list = list(asks) if asks is not None else []
        if ask_list:
            prices = [_price_from_level(x) for x in ask_list]
            best_ask = min(prices)
    except (TypeError, ValueError):
        pass
    return best_bid, best_ask


def load_snapshots_for_slug(data_dir: Path, slug: str, max_files: int | None = None) -> pd.DataFrame:
    """加载该 slug 下所有 orderbook 快照 parquet，合并为一张表。"""
    snap_dir = data_dir / "orderbook" / "snapshots" / slug
    if not snap_dir.is_dir():
        return pd.DataFrame()
    files = sorted(snap_dir.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    if max_files is not None:
        files = files[: max_files]
    dfs = [pd.read_parquet(p) for p in files]
    out = pd.concat(dfs, ignore_index=True)
    # 保证 asset_id 为字符串，便于与 meta 的 token_ids 匹配
    if "asset_id" in out.columns:
        out["asset_id"] = out["asset_id"].astype(str).str.strip()
    # 丢弃 bids/asks 为空或无效的行，避免污染 best_bid/best_ask
    def _has_valid_book(books):
        if books is None or (isinstance(books, float) and pd.isna(books)):
            return False
        try:
            L = list(books)
            return len(L) > 0
        except (TypeError, ValueError):
            return False

    mask = out.apply(lambda r: _has_valid_book(r.get("bids")) and _has_valid_book(r.get("asks")), axis=1)
    out = out.loc[mask].copy()
    return out


def build_asset_to_outcome(meta: dict) -> dict[str, str]:
    """token_ids 与 outcomes 顺序对应，返回 asset_id -> 'Up'|'Down'。"""
    token_ids = meta.get("token_ids") or []
    outcomes = meta.get("outcomes") or ["Up", "Down"]
    return dict(zip(token_ids, outcomes[: len(token_ids)]))


def run_arbitrage_analysis(
    data_dir: Path,
    slug: str,
    *,
    buy_threshold: float = 0.99,
    sell_threshold: float = 1.01,
    max_files: int | None = None,
) -> pd.DataFrame:
    """
    对指定 slug 做套利分析：合并同一 ts_ms 下两个 asset 的盘口，计算 mid/spread 与 sum_ask/sum_bid。
    返回每时刻一行的分析结果 DataFrame。
    """
    meta = load_market_meta(data_dir, slug)
    asset_to_outcome = build_asset_to_outcome(meta)
    if len(asset_to_outcome) != 2:
        raise ValueError(f"Expected 2 tokens for slug {slug}, got {len(asset_to_outcome)}")

    df = load_snapshots_for_slug(data_dir, slug, max_files=max_files)
    if df.empty:
        return pd.DataFrame()

    # 解析 best bid / best ask
    def row_best_bid_ask(row):
        bids = row["bids"] if isinstance(row["bids"], list) else []
        asks = row["asks"] if isinstance(row["asks"], list) else []
        b, a = _best_bid_ask(bids, asks)
        return pd.Series({"best_bid": b, "best_ask": a})

    df[["best_bid", "best_ask"]] = df.apply(row_best_bid_ask, axis=1)
    # 与 meta 的 token_ids（字符串）一致
    df["outcome"] = df["asset_id"].map(asset_to_outcome)

    # 按 ts_ms 对齐：同一时刻两个 outcome 各一行，pivot 成一行两列
    by_ts = df.groupby("ts_ms")
    rows = []
    for ts_ms, grp in by_ts:
        if grp.shape[0] != 2:
            continue
        up_row = grp[grp["outcome"] == "Up"]
        down_row = grp[grp["outcome"] == "Down"]
        if up_row.empty or down_row.empty:
            continue
        up_row, down_row = up_row.iloc[0], down_row.iloc[0]
        best_bid_up = float(up_row["best_bid"])
        best_ask_up = float(up_row["best_ask"])
        best_bid_down = float(down_row["best_bid"])
        best_ask_down = float(down_row["best_ask"])

        mid_up = (best_bid_up + best_ask_up) / 2 if (best_bid_up + best_ask_up) > 0 else 0.0
        mid_down = (best_bid_down + best_ask_down) / 2 if (best_bid_down + best_ask_down) > 0 else 0.0
        spread_up = best_ask_up - best_bid_up if best_ask_up > best_bid_up else 0.0
        spread_down = best_ask_down - best_bid_down if best_ask_down > best_bid_down else 0.0

        sum_ask = best_ask_up + best_ask_down
        sum_bid = best_bid_up + best_bid_down

        buy_both = sum_ask < buy_threshold
        sell_both = sum_bid > sell_threshold

        rows.append({
            "slug": slug,
            "ts_ms": ts_ms,
            "best_bid_up": best_bid_up,
            "best_ask_up": best_ask_up,
            "best_bid_down": best_bid_down,
            "best_ask_down": best_ask_down,
            "mid_up": mid_up,
            "mid_down": mid_down,
            "spread_up": spread_up,
            "spread_down": spread_down,
            "sum_bid": sum_bid,
            "sum_ask": sum_ask,
            "buy_both_opportunity": buy_both,
            "sell_both_opportunity": sell_both,
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def send_arbitrage_signals_to_discord(
    webhook_url: str,
    result: pd.DataFrame,
    buy_threshold: float,
    sell_threshold: float,
    *,
    max_opportunities_in_embed: int = 10,
) -> None:
    """将套利信号汇总发送到 Discord webhook。"""
    opportunities = result[
        result["buy_both_opportunity"] | result["sell_both_opportunity"]
    ].copy()
    if opportunities.empty:
        return

    n_buy = int(result["buy_both_opportunity"].sum())
    n_sell = int(result["sell_both_opportunity"].sum())
    slugs = result["slug"].unique().tolist()

    def _ts_fmt(ts_ms):
        try:
            from datetime import datetime
            return datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%H:%M:%S")
        except Exception:
            return str(ts_ms)

    fields = [
        {"name": "市场 (slug)", "value": ", ".join(slugs[:5]) + (" ..." if len(slugs) > 5 else ""), "inline": False},
        {"name": "买双套利 (sum_ask < {:.3f})".format(buy_threshold), "value": f"**{n_buy}** 次", "inline": True},
        {"name": "卖双套利 (sum_bid > {:.3f})".format(sell_threshold), "value": f"**{n_sell}** 次", "inline": True},
        {"name": "时间点数", "value": str(len(result)), "inline": True},
    ]

    # 取前几条机会明细
    for _, row in opportunities.head(max_opportunities_in_embed).iterrows():
        kind = "买双" if row["buy_both_opportunity"] else "卖双"
        ts_str = _ts_fmt(row["ts_ms"])
        val = f"sum_ask={row['sum_ask']:.4f} sum_bid={row['sum_bid']:.4f}"
        fields.append({"name": f"{kind} @ {ts_str} ({row['slug']})", "value": val, "inline": False})

    body = {
        "embeds": [
            {
                "title": "🔔 Polymarket 套利信号",
                "description": "BTC 5min Up/Down 市场检测到套利机会。",
                "color": 0x00FF00 if (n_buy or n_sell) else 0x808080,
                "fields": fields,
            }
        ]
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.post(webhook_url, json=body)
            r.raise_for_status()
    except Exception as e:
        print(f"Discord webhook 发送失败: {e}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polymarket BTC 5min 套利信号分析：基于 orderbook 快照计算 mid/spread 与买双/卖双套利机会。"
    )
    parser.add_argument("--data-dir", type=Path, default=None, help="数据目录，默认使用 config 或 data/")
    parser.add_argument("--slug", type=str, default=None, help="指定市场 slug；不指定则分析所有已有快照的 btc-updown-5m 市场")
    parser.add_argument("--output", "-o", type=Path, default=None, help="将分析结果写入 CSV")
    parser.add_argument("--threshold", type=float, default=0.01, help="套利阈值：买双为 sum_ask < (1-threshold)，卖双为 sum_bid > (1+threshold)")
    parser.add_argument("--summary-only", action="store_true", help="只打印汇总统计，不输出每时刻明细")
    parser.add_argument("--max-files", type=int, default=None, help="每个市场最多加载的 parquet 文件数（用于调试）")
    parser.add_argument("--discord-webhook", type=str, default=None, help="Discord webhook URL，套利机会将发送到此频道；也可设置环境变量 DISCORD_WEBHOOK_URL 或 config 中 discord_webhook_url")
    args = parser.parse_args()

    data_dir = args.data_dir
    if data_dir is None and load_config is not None:
        cfg = load_config()
        data_dir = Path(cfg.get("data_dir", PROJECT_ROOT / "data"))
    if data_dir is None:
        data_dir = PROJECT_ROOT / "data"
    data_dir = Path(data_dir)
    _ensure_data_dir(data_dir)

    buy_threshold = 1.0 - args.threshold
    sell_threshold = 1.0 + args.threshold

    if args.slug:
        slugs = [args.slug]
        for s in slugs:
            if not (data_dir / "markets" / f"meta_{s}.json").exists():
                print(f"Error: meta not found for slug {args.slug}", file=sys.stderr)
                return 1
    else:
        slugs = list_slugs_from_meta(data_dir)
        # 只保留有快照数据的 slug
        slugs = [s for s in slugs if (data_dir / "orderbook" / "snapshots" / s).is_dir()]
        if not slugs:
            print("No btc-updown-5m slugs with snapshot data found.", file=sys.stderr)
            return 1

    all_dfs: list[pd.DataFrame] = []
    for slug in slugs:
        try:
            df = run_arbitrage_analysis(
                data_dir,
                slug,
                buy_threshold=buy_threshold,
                sell_threshold=sell_threshold,
                max_files=args.max_files,
            )
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"Skip {slug}: {e}", file=sys.stderr)

    if not all_dfs:
        print("No snapshot data could be analyzed.", file=sys.stderr)
        return 1

    result = pd.concat(all_dfs, ignore_index=True)
    result = result.sort_values("ts_ms").reset_index(drop=True)

    # 汇总
    n = len(result)
    n_buy = result["buy_both_opportunity"].sum()
    n_sell = result["sell_both_opportunity"].sum()
    print("=== 套利信号分析汇总 ===")
    print(f"数据目录: {data_dir}")
    print(f"市场( slug ): {result['slug'].unique().tolist()}")
    print(f"时间点数: {n}")
    print(f"买双套利机会 (sum_ask < {buy_threshold:.3f}): {int(n_buy)} 次")
    print(f"卖双套利机会 (sum_bid > {sell_threshold:.3f}): {int(n_sell)} 次")
    print(f"sum_ask  Min={result['sum_ask'].min():.4f}  Mean={result['sum_ask'].mean():.4f}  Max={result['sum_ask'].max():.4f}")
    print(f"sum_bid  Min={result['sum_bid'].min():.4f}  Mean={result['sum_bid'].mean():.4f}  Max={result['sum_bid'].max():.4f}")

    if not args.summary_only:
        print("\n--- 前 20 条明细 ---")
        print(result.head(20).to_string())

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(args.output, index=False)
        print(f"\n已写入: {args.output}")

    # Discord 通知：有套利机会且配置了 webhook 时发送
    discord_url = args.discord_webhook
    if not discord_url and load_config is not None:
        cfg = load_config()
        discord_url = (cfg.get("discord_webhook_url") or "").strip()
    if discord_url and (n_buy > 0 or n_sell > 0):
        send_arbitrage_signals_to_discord(
            discord_url, result, buy_threshold, sell_threshold
        )
        print("已向 Discord 发送套利信号。")

    return 0


if __name__ == "__main__":
    sys.exit(main())
