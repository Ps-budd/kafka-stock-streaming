import os
import asyncio
import json
from datetime import datetime

import asyncpg
from aiokafka import AIOKafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
GROUP_ID = os.getenv("GROUP_ID", "db-writer")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/stocks")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stock_prices (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    source TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ts ON stock_prices(ts);
"""


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


async def handle_message(pool: asyncpg.Pool, value_bytes: bytes) -> None:
    try:
        data = json.loads(value_bytes.decode("utf-8"))
    except Exception:
        return
    symbol = data.get("symbol")
    price = data.get("price")
    ts_str = data.get("ts")
    source = data.get("source")
    if symbol is None or price is None or ts_str is None:
        return

    # Parse ISO 8601 string to datetime with timezone
    try:
        # Support both "+00:00" and "Z"
        ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO stock_prices(symbol, price, ts, source)
            VALUES($1, $2, $3, $4)
            """,
            symbol, float(price), ts_dt, source,
        )


async def run() -> None:
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    await ensure_schema(pool)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await consumer.start()
    print(f"[db] started. topic={KAFKA_TOPIC} group={GROUP_ID} dsn={POSTGRES_DSN}")
    try:
        async for msg in consumer:
            await handle_message(pool, msg.value)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[db] shutdown")


