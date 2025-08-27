# Kafka Stock Streaming (TSLA prototype)

Stream stock price ticks from Polygon.io into Kafka, persist to Postgres, and compute SMA-based alerts.

## Architecture
```
Producer (Polygon.io) --> Kafka topic: stock_prices --> [DB Consumer] --> Postgres
                                                 └--> [Alerts Consumer] --> console + (optional) stock_alerts
```

## Prerequisites
- Docker + Docker Compose
- Python 3.10+

## Start infra
```bash
cd kafka-stock-streaming
docker compose up -d
```

## Install deps
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment
```bash
export POLYGON_API_KEY=YOUR_POLYGON_KEY
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=stock_prices
export SYMBOLS=TSLA,AAPL,MSFT,AMZN,GOOGL   # comma-separated
export INTERVAL_SECONDS=5                  # request cadence (round-robin across symbols)
export POSTGRES_DSN=postgresql://postgres:postgres@localhost:5432/stocks
export SMA_WINDOW=20
export ZSCORE_THRESHOLD=2.0
```

Or use a `.env` file in this directory (auto-loaded by the producer):
```
POLYGON_API_KEY=YOUR_POLYGON_KEY
SYMBOLS=TSLA,AAPL,MSFT,AMZN,GOOGL
INTERVAL_SECONDS=5
POSTGRES_DSN=postgresql://postgres:postgres@localhost:5432/stocks
```

## Run services (separate terminals)
```bash
python producer.py      # publishes round-robin for SYMBOLS with INTERVAL_SECONDS between requests
python consumer_db.py   # writes to Postgres
python consumer_alerts.py
```

## Test Alpha Vantage
```bash
curl -s 'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=TSLA&apikey=YOUR_KEY' | jq '.'
```

## Notes
- Kafka auto-creates `stock_prices` on first publish.
- Alerts consumer optionally publishes to `stock_alerts` if `PUBLISH_ALERTS=true`.
- Point to external Kafka by changing `KAFKA_BOOTSTRAP_SERVERS`.
