import os
import asyncio
import json
from collections import defaultdict, deque
from statistics import mean, pstdev

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
GROUP_ID = os.getenv("GROUP_ID_ALERTS", "analytics")
SMA_WINDOW = int(os.getenv("SMA_WINDOW", "20"))
ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "2.0"))
PUBLISH_ALERTS = os.getenv("PUBLISH_ALERTS", "false").lower() in {"1", "true", "yes"}
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "stock_alerts")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/stocks")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stock_alerts (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    sma DOUBLE PRECISION NOT NULL,
    zscore DOUBLE PRECISION NOT NULL,
    rule TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stock_alerts_symbol ON stock_alerts(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_alerts_ts ON stock_alerts(ts);
"""

async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


async def run() -> None:
    db_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    await ensure_schema(db_pool)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await consumer.start()

    producer = None
    if PUBLISH_ALERTS:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()

    print(f"[alerts] started. topic={KAFKA_TOPIC} window={SMA_WINDOW} z>={ZSCORE_THRESHOLD}")
    windows: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=SMA_WINDOW))

    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
            except Exception:
                continue

            symbol = data.get("symbol")
            price = data.get("price")
            ts = data.get("ts")
            if symbol is None or price is None:
                continue

            window = windows[symbol]
            window.append(float(price))
            if len(window) < SMA_WINDOW:
                continue

            sma = mean(window)
            std = pstdev(window) if len(window) > 1 else 0.0
            if std <= 0:
                continue
            z = (float(price) - sma) / std
            if abs(z) >= ZSCORE_THRESHOLD:
                alert = {
                    "symbol": symbol,
                    "price": float(price),
                    "sma": sma,
                    "zscore": z,
                    "rule": f"abs(z)>={ZSCORE_THRESHOLD}",
                    "ts": ts,
                }
                print(f"[alerts] ALERT: {alert}")
                # persist to Postgres
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO stock_alerts(symbol, price, sma, zscore, rule, ts)
                            VALUES($1, $2, $3, $4, $5, $6::timestamptz)
                            """,
                            alert["symbol"], alert["price"], alert["sma"], alert["zscore"], alert["rule"], alert["ts"],
                        )
                except Exception as db_exc:
                    print(f"[alerts] db error: {db_exc}")
                if producer is not None:
                    try:
                        await producer.send_and_wait(ALERTS_TOPIC, value=json.dumps(alert).encode("utf-8"), key=symbol.encode("utf-8"))
                    except Exception:
                        pass
    finally:
        await consumer.stop()
        if producer is not None:
            await producer.stop()
        await db_pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[alerts] shutdown")


