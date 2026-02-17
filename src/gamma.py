"""
Gamma API client: discover current and next BTC 5min up/down markets.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

BTC_5M_SLUG_PREFIX = "btc-updown-5m"
GAMMA_BASE = "https://gamma-api.polymarket.com"

logger = logging.getLogger(__name__)


@dataclass
class MarketInfo:
    """Single 5min window market: slug, token_ids, endDate (UTC)."""

    slug: str
    condition_id: str
    end_date_utc: datetime
    end_date_ts_ms: int
    token_ids: list[str]
    # outcome order may vary; token_ids[0] often Up, [1] Down - check market.outcomes if needed
    outcomes: list[str]  # e.g. ["Up", "Down"]

    @property
    def asset_ids(self) -> list[str]:
        return self.token_ids


def _parse_clob_token_ids(market: dict[str, Any]) -> list[str]:
    raw = market.get("clobTokenIds")
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    s = str(raw).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _parse_end_date(market: dict[str, Any]) -> tuple[datetime, int]:
    for key in ("endDate", "end_date_iso", "endDateIso"):
        v = market.get(key)
        if not v:
            continue
        try:
            if isinstance(v, str) and "T" in v:
                if v.endswith("Z"):
                    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(v)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                return dt, int(dt.timestamp() * 1000)
        except Exception as e:
            logger.warning("Failed to parse endDate %s: %s", v, e)
    return datetime.now(timezone.utc), 0


def _event_to_market_info(event: dict[str, Any]) -> MarketInfo | None:
    slug = event.get("slug") or ""
    if not slug.startswith(BTC_5M_SLUG_PREFIX):
        return None
    markets = event.get("markets") or []
    if not markets:
        logger.warning("Event %s has no markets", slug)
        return None
    market = markets[0]
    token_ids = _parse_clob_token_ids(market)
    if len(token_ids) < 2:
        logger.warning("Event %s market has invalid clobTokenIds: %s", slug, market.get("clobTokenIds"))
        return None
    end_dt, end_ts_ms = _parse_end_date(market)
    condition_id = (market.get("conditionId") or "").strip()
    outcomes_raw = market.get("outcomes") or market.get("shortOutcomes") or "Up,Down"
    if isinstance(outcomes_raw, str):
        outcomes = [x.strip() for x in outcomes_raw.split(",") if x.strip()]
    else:
        outcomes = list(outcomes_raw) if outcomes_raw else ["Up", "Down"]
    if len(outcomes) < 2:
        outcomes = ["Up", "Down"]
    return MarketInfo(
        slug=slug,
        condition_id=condition_id,
        end_date_utc=end_dt,
        end_date_ts_ms=end_ts_ms,
        token_ids=token_ids,
        outcomes=outcomes[:2],
    )


async def get_btc_5m_events(
    client: httpx.AsyncClient,
    *,
    limit: int = 80,
    base_url: str = GAMMA_BASE,
) -> list[MarketInfo]:
    """
    Fetch active events that are BTC 5min up/down, ordered by endDate ascending.
    Uses end_date_min=now so we get markets that have not yet ended.
    """
    now = datetime.now(timezone.utc)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    url = f"{base_url.rstrip('/')}/events"
    params = {
        "closed": "false",
        "limit": limit,
        "order": "endDate",
        "ascending": "true",
        "end_date_min": now_iso,
    }
    resp = await client.get(url, params=params, timeout=20.0)
    resp.raise_for_status()
    events = resp.json()
    if not isinstance(events, list):
        return []
    result: list[MarketInfo] = []
    for ev in events:
        info = _event_to_market_info(ev)
        if info is not None:
            result.append(info)
    return result


async def get_current_and_next_btc_5m(
    *,
    base_url: str = GAMMA_BASE,
    limit: int = 80,
    retries: int = 3,
    retry_delay: float = 5.0,
) -> tuple[MarketInfo | None, MarketInfo | None]:
    """
    Return (current_market, next_market) for BTC 5min.
    Current = nearest btc-updown-5m event that has not yet ended (smallest endDate in future).
    Next = the one after current, if any.
    Retries on connection/timeout errors.
    """
    last_error: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                markets = await get_btc_5m_events(client, limit=limit, base_url=base_url)
            if not markets:
                return None, None
            current = markets[0]
            next_m = markets[1] if len(markets) > 1 else None
            return current, next_m
        except (httpx.ConnectError, httpx.TimeoutException, OSError) as e:
            last_error = e
            if attempt < retries - 1:
                logger.warning("Gamma API attempt %s failed: %s; retry in %.0fs", attempt + 1, e, retry_delay)
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Gamma API failed after %s attempts: %s", retries, e)
                raise
    if last_error:
        raise last_error
    return None, None


def get_event_by_slug_sync(slug: str, base_url: str = GAMMA_BASE) -> MarketInfo | None:
    """
    Fetch a single event by slug (e.g. btc-updown-5m-1771342500).
    Useful for way B: compute next window start_ts and request by slug.
    """
    url = f"{base_url.rstrip('/')}/events/slug/{slug}"
    try:
        with httpx.Client(timeout=10.0) as c:
            resp = c.get(url)
            resp.raise_for_status()
            event = resp.json()
    except Exception as e:
        logger.warning("get_event_by_slug %s failed: %s", slug, e)
        return None
    return _event_to_market_info(event)
