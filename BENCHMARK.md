# Benchmark Guide

## 0) One-command soak run (recommended)

Run once, come back in 1-2 hours, and read saved results:

```bash
chmod +x scripts/run_soak_benchmark.sh scripts/read_load.py

# 1 hour default
./scripts/run_soak_benchmark.sh

# 2 hour run example
DURATION_SECONDS=7200 ./scripts/run_soak_benchmark.sh
```

Output is written to `benchmarks/soak_<timestamp>/`:

- `summary.md`
- `summary.json`
- `write.log`
- `read.log`

## 1) Start stack

```bash
docker compose up -d --build
```

## 2) Run synthetic load (bypasses provider limits)

Example: 1000 symbols, 5000 events/sec, 60s run.

```bash
python scripts/load_generate.py \
  --bootstrap-servers localhost:29092 \
  --topic price-events \
  --provider bench \
  --symbols 1000 \
  --rate 5000 \
  --duration 60
```

Output format:

- `events_sent=<int>`
- `errors=<int>`
- `elapsed_seconds=<float>`
- `achieved_throughput_eps=<float>`

## 3) What to inspect in Grafana

Open `http://localhost:3000` and dashboard **Market Service Performance**.

Key panels:

- API `/prices/latest` p50/p95/p99 latency (cached=true/false)
- Cache hit ratio
- Consumer lag
- Pipeline end-to-end p95
- Events/sec processed

## 4) Scaling notes

- Increase `KAFKA_TOPIC_PARTITIONS` to raise parallelism for distinct `(provider,symbol)` keys.
- Scale consumer replicas:

```bash
docker compose up -d --scale consumer=3
```

- Throughput scales when partition count >= consumer replicas and key distribution is wide.
- MA5 state is in-memory per consumer instance; on consumer restart, rolling windows reset and refill from new events.
