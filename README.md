# Market Data Service

FastAPI microservice that:
- Fetches market prices from an external provider (`yahoo`, `alpha_vantage`, `finnhub`)
- Persists raw and processed data in Postgres
- Publishes fetched prices to Kafka topic `price-events`
- Consumes those events in a separate worker to compute 5-point moving averages
- Exposes APIs for latest price reads and polling job creation
- Uses Redis as an optional read-through cache
- Exposes metrics for Prometheus/Grafana

## APIs

- `GET /prices/latest?symbol=AAPL&provider=yahoo`
- `POST /prices/poll` with body:

```json
{
  "symbols": ["AAPL", "MSFT"],
  "interval": 60,
  "provider": "yahoo"
}
```

Returns `202 Accepted`:

```json
{
  "job_id": "<uuid>",
  "status": "accepted"
}
```

- `GET /prices/poll/{job_id}`
- `GET /health`
- `GET /metrics`

## Architecture

- **API service** (`app/main.py`)
  - Fetches provider data
  - Writes `raw_market_data` + `price_points`
  - Publishes Kafka events
  - Caches latest reads in Redis
  - Schedules polling jobs using APScheduler

- **Worker service** (`worker/consumer.py`)
  - Consumes `price-events`
  - Processes events in batches (single DB transaction per batch)
  - Computes MA5 in O(1) using in-memory rolling windows per `(provider,symbol)`
  - Upserts into `symbol_averages` and `latest_prices`
  - Uses idempotent event tracking (`processed_price_events`) to handle replays safely

## Data model

- `raw_market_data`
  - Raw provider responses (JSONB), provider metadata, fetch timestamp
- `price_points`
  - Normalized price points with symbol/provider/as_of and raw reference id
- `symbol_averages`
  - Latest moving average row per `(symbol, provider, window_size)`
- `latest_prices`
  - O(1) read model for latest price + MA5 by `(provider, symbol)`
- `processed_price_events`
  - Idempotency table keyed by `price_point_id` for replay-safe consumer writes
- `polling_jobs`
  - Persistent polling job configs and run state

Indexes are included for symbol/time lookup patterns.

## Local run with Docker Compose

1. Copy environment file:

```bash
cp .env.example .env
```

2. If you want non-Yahoo providers, set keys in `.env`:
- `ALPHA_VANTAGE_API_KEY`
- `FINNHUB_API_KEY`

Rate-limit knobs (important for Yahoo):
- `PROVIDER_RATE_LIMIT_PER_MINUTE`
- `PROVIDER_HTTP_MAX_RETRIES`
- `PROVIDER_HTTP_BACKOFF_SECONDS`
- `ENABLE_YAHOO_RATE_LIMIT_FALLBACK`
- `YAHOO_RATE_LIMIT_FALLBACK_PROVIDER` (set to `alpha_vantage`)

Consumer throughput knobs:
- `CONSUMER_BATCH_SIZE`
- `CONSUMER_POLL_TIMEOUT_MS`
- `CONSUMER_RETRY_BACKOFF_SECONDS`

Kafka partitioning:
- `KAFKA_TOPIC_PARTITIONS`

3. Start stack:

```bash
docker compose up --build
```

4. Endpoints:
- API: `http://localhost:8000`
- Adminer: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## Example calls

```bash
curl "http://localhost:8000/prices/latest?symbol=AAPL&provider=yahoo"
```

```bash
curl -X POST "http://localhost:8000/prices/poll" \
  -H "Content-Type: application/json" \
  -d '{"symbols":["AAPL","MSFT"],"interval":60,"provider":"yahoo"}'
```

## One-command smoke test

```bash
./scripts/smoke_test.sh
```

Optional overrides:

```bash
API_URL=http://localhost:8000 PROVIDER=yahoo PRIMARY_SYMBOL=AAPL MA_SYMBOL=MSFT ./scripts/smoke_test.sh
```

If provider calls are heavily rate-limited, keep `MIN_INTERVAL` at 60+ seconds in the smoke test.

## Monitoring metrics

Prometheus scrapes:
- API metrics at `api:8000/metrics`
- Worker metrics at `consumer:9108/metrics`

Custom metrics include:
- `http_request_duration_seconds{route,method,status}`
- `latest_price_request_duration_seconds{cached,status}`
- provider call latency/error/rate-limit metrics
- `cache_hit_total`, `cache_miss_total`
- `events_processed_total`
- `consumer_process_duration_seconds`
- `price_pipeline_end_to_end_seconds`
- `db_write_duration_seconds`
- `kafka_consumer_lag`

## Benchmarking

See `BENCHMARK.md` for synthetic load commands and dashboard interpretation.

## Migration

Manual SQL migration for scaling/performance updates:

- `migrations/20260218_scaling_perf.sql`

## Development without Docker

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

In another terminal:

```bash
python -m worker.consumer
```

(Requires local Postgres, Kafka, and Redis configured via `.env`.)

Note: MA5 rolling windows are in-memory per consumer instance and reset on consumer restart.
