import os
import asyncio
import json
from datetime import datetime, timezone, timedelta

from aiokafka import AIOKafkaProducer
from typing import Optional
from dotenv import load_dotenv
from pathlib import Path
import httpx


# Load .env from this script directory and override existing env vars
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
POLYGON_BASE_URL = os.getenv("POLYGON_BASE_URL", "https://api.polygon.io")
# Support multiple symbols via comma-separated list
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", os.getenv("SYMBOL", "TSLA,AAPL,MSFT,AMZN,GOOGL")).split(",") if s.strip()]
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
# Global request pacing interval between API calls (round-robin across symbols)
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "5"))


async def fetch_price_polygon(symbol: str) -> Optional[float]:
    """Fetch latest trade price for a symbol from Polygon.io REST.
    Uses GET /v2/last/trade/{symbol}
    """
    if not POLYGON_API_KEY:
        print("[producer] Missing POLYGON_API_KEY")
        return None
    url = f"{POLYGON_BASE_URL}/v2/last/trade/{symbol}"
    params = {"apiKey": POLYGON_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                last = data.get("last") or {}
                price = last.get("price") or last.get("p")
                return float(price) if price is not None else None
            # Fallback for unauthorized real-time: use latest 1-min aggregate
            if resp.status_code in (401, 403):
                return await fetch_latest_agg_price(client, symbol)
            print(f"[producer] polygon {symbol} HTTP {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as exc:
        print(f"[producer] polygon http error for {symbol}: {exc}")
        return None


async def fetch_latest_agg_price(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    """Fetch the most recent 1-minute aggregate close as a proxy for price.
    Uses: /v2/aggs/ticker/{symbol}/range/1/minute/{from}/{to}?sort=desc&limit=1
    """
    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=7)
    url = f"{POLYGON_BASE_URL}/v2/aggs/ticker/{symbol}/range/1/minute/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "desc",
        "limit": 1,
        "apiKey": POLYGON_API_KEY,
    }
    r = await client.get(url, params=params)
    if r.status_code != 200:
        print(f"[producer] polygon aggs {symbol} HTTP {r.status_code}: {r.text[:200]}")
        return None
    data = r.json() or {}
    results = data.get("results") or []
    if not results:
        return None
    # 'c' is close price
    c = results[0].get("c")
    try:
        return float(c) if c is not None else None
    except Exception:
        return None


async def run() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    print(f"[producer] started. topic={KAFKA_TOPIC} symbols={SYMBOLS} interval={INTERVAL_SECONDS}s broker={KAFKA_BOOTSTRAP_SERVERS}")
    try:
        idx = 0
        while True:
            symbol = SYMBOLS[idx % len(SYMBOLS)]
            idx += 1
            price = await fetch_price_polygon(symbol)
            if price is not None:
                msg = {
                    "symbol": symbol,
                    "price": price,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "source": "polygon",
                }
                value = json.dumps(msg).encode("utf-8")
                key = symbol.encode("utf-8")
                try:
                    await producer.send_and_wait(KAFKA_TOPIC, value=value, key=key)
                    print(f"[producer] published {msg}")
                except Exception as exc:
                    print(f"[producer] kafka error: {exc}")
            await asyncio.sleep(INTERVAL_SECONDS)
    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[producer] shutdown")


