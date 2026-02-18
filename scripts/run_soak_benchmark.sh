#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'EOF'
Usage: run_soak_benchmark.sh

Runs an unattended soak benchmark and writes results under benchmarks/soak_<timestamp>/.

Configuration is environment-variable driven:
  DURATION_SECONDS      default: 3600
  WRITE_RATE            default: 5000
  WRITE_SYMBOLS         default: 2000
  READ_RPS              default: 500
  READ_THREADS          default: 50
  READ_HOT_SYMBOLS      default: 200
  PROVIDER              default: bench
  TOPIC                 default: price-events
  API_BASE_URL          default: http://localhost:8000
  PROM_URL              default: http://localhost:9090
  PRESEED_SECONDS       default: 5
  LOG_INTERVAL_SECONDS  default: 30
  OUT_DIR               default: benchmarks/soak_<timestamp>

Examples:
  ./scripts/run_soak_benchmark.sh
  DURATION_SECONDS=7200 ./scripts/run_soak_benchmark.sh
  DURATION_SECONDS=3600 WRITE_RATE=8000 READ_RPS=1200 ./scripts/run_soak_benchmark.sh
EOF
  exit 0
fi

if [[ "$#" -gt 0 ]]; then
  echo "This script does not accept positional args. Use env vars or --help." >&2
  exit 1
fi

DURATION_SECONDS="${DURATION_SECONDS:-3600}"
WRITE_RATE="${WRITE_RATE:-5000}"
WRITE_SYMBOLS="${WRITE_SYMBOLS:-2000}"
READ_RPS="${READ_RPS:-500}"
READ_THREADS="${READ_THREADS:-50}"
READ_HOT_SYMBOLS="${READ_HOT_SYMBOLS:-200}"
PROVIDER="${PROVIDER:-bench}"
TOPIC="${TOPIC:-price-events}"
API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PRESEED_SECONDS="${PRESEED_SECONDS:-5}"
LOG_INTERVAL_SECONDS="${LOG_INTERVAL_SECONDS:-30}"
OUT_DIR="${OUT_DIR:-benchmarks/soak_$(date +%Y%m%d_%H%M%S)}"

WRITE_LOG="$OUT_DIR/write.log"
READ_LOG="$OUT_DIR/read.log"
SUMMARY_JSON="$OUT_DIR/summary.json"
SUMMARY_MD="$OUT_DIR/summary.md"

mkdir -p "$OUT_DIR"

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd" >&2
    exit 1
  fi
}

prom_query_scalar() {
  local expr="$1"
  curl -sG "$PROM_URL/api/v1/query" --data-urlencode "query=$expr" | python3 -c '
import json, sys
try:
    payload = json.load(sys.stdin)
    result = payload.get("data", {}).get("result", [])
    if not result:
        print("NaN")
    else:
        print(result[0]["value"][1])
except Exception:
    print("NaN")
'
}

read_json_value() {
  local file="$1"
  local path="$2"
  python3 - "$file" "$path" <<'PY'
import json
import sys

filename = sys.argv[1]
path = sys.argv[2].split(".")
text = open(filename, "r", encoding="utf-8").read()
start = text.find("{")
end = text.rfind("}")
if start == -1 or end == -1:
    print("NaN")
    sys.exit(0)

obj = json.loads(text[start:end + 1])
value = obj
for part in path:
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
if value is None:
    print("NaN")
else:
    print(value)
PY
}

require_cmd docker
require_cmd curl
require_cmd python3

log "Output directory: $OUT_DIR"
log "Ensuring core services are running"
docker compose up -d api consumer kafka postgres redis prometheus grafana >/dev/null

log "Waiting for API and Prometheus"
for _ in {1..60}; do
  if curl -fsS "$API_BASE_URL/health" >/dev/null 2>&1 && curl -fsS "$PROM_URL/-/healthy" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! curl -fsS "$API_BASE_URL/health" >/dev/null 2>&1; then
  echo "API did not become ready" >&2
  exit 1
fi

if ! curl -fsS "$PROM_URL/-/healthy" >/dev/null 2>&1; then
  echo "Prometheus did not become ready" >&2
  exit 1
fi

log "Disabling active polling jobs to avoid provider background noise"
docker compose exec -T postgres psql -U postgres -d market_data -c \
  "UPDATE polling_jobs SET is_active=false WHERE is_active=true;" >/dev/null

PRESEED_RATE="$WRITE_RATE"
if (( PRESEED_RATE > 1000 )); then
  PRESEED_RATE=1000
fi

log "Pre-seeding latest_prices for provider=${PROVIDER} (${PRESEED_SECONDS}s)"
docker compose exec -T api python - \
  --bootstrap-servers kafka:9092 \
  --topic "$TOPIC" \
  --provider "$PROVIDER" \
  --symbols "$WRITE_SYMBOLS" \
  --rate "$PRESEED_RATE" \
  --duration "$PRESEED_SECONDS" < scripts/load_generate.py >/dev/null

WRITE_PID=""
READ_PID=""
cleanup() {
  if [[ -n "$WRITE_PID" ]] && kill -0 "$WRITE_PID" >/dev/null 2>&1; then
    kill "$WRITE_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$READ_PID" ]] && kill -0 "$READ_PID" >/dev/null 2>&1; then
    kill "$READ_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup INT TERM

log "Starting write load for ${DURATION_SECONDS}s at ${WRITE_RATE} events/sec"
docker compose exec -T api python - \
  --bootstrap-servers kafka:9092 \
  --topic "$TOPIC" \
  --provider "$PROVIDER" \
  --symbols "$WRITE_SYMBOLS" \
  --rate "$WRITE_RATE" \
  --duration "$DURATION_SECONDS" < scripts/load_generate.py >"$WRITE_LOG" 2>&1 &
WRITE_PID=$!

log "Starting read load for ${DURATION_SECONDS}s at ${READ_RPS} req/sec"
docker compose exec -T api python - \
  --base-url "$API_BASE_URL" \
  --provider "$PROVIDER" \
  --duration "$DURATION_SECONDS" \
  --total-rps "$READ_RPS" \
  --threads "$READ_THREADS" \
  --symbol-pool "$WRITE_SYMBOLS" \
  --hot-symbols "$READ_HOT_SYMBOLS" < scripts/read_load.py >"$READ_LOG" 2>&1 &
READ_PID=$!

START_TS="$(date +%s)"
while true; do
  alive=0
  if kill -0 "$WRITE_PID" >/dev/null 2>&1; then
    alive=1
  fi
  if kill -0 "$READ_PID" >/dev/null 2>&1; then
    alive=1
  fi
  if [[ "$alive" -eq 0 ]]; then
    break
  fi
  now="$(date +%s)"
  elapsed=$((now - START_TS))
  log "Benchmark running... elapsed=${elapsed}s"
  sleep "$LOG_INTERVAL_SECONDS"
done

wait "$WRITE_PID"
wait "$READ_PID"
trap - INT TERM

log "Collecting results"
write_events_sent="$(grep -E '^events_sent=' "$WRITE_LOG" | tail -1 | cut -d= -f2 || true)"
write_errors="$(grep -E '^errors=' "$WRITE_LOG" | tail -1 | cut -d= -f2 || true)"
write_throughput="$(grep -E '^achieved_throughput_eps=' "$WRITE_LOG" | tail -1 | cut -d= -f2 || true)"

if [[ -z "${write_events_sent}" ]]; then
  write_events_sent="NaN"
fi
if [[ -z "${write_errors}" ]]; then
  write_errors="NaN"
fi
if [[ -z "${write_throughput}" ]]; then
  write_throughput="NaN"
fi

read_total="$(read_json_value "$READ_LOG" "total_requests")"
read_success="$(read_json_value "$READ_LOG" "success_requests")"
read_errors="$(read_json_value "$READ_LOG" "error_requests")"
read_achieved_rps="$(read_json_value "$READ_LOG" "achieved_rps")"
read_p95_ms="$(read_json_value "$READ_LOG" "latency_ms.p95_approx")"
read_p99_ms="$(read_json_value "$READ_LOG" "latency_ms.p99_approx")"

prom_events_rate="$(prom_query_scalar 'sum(rate(events_processed_total[5m]))')"
prom_cache_ratio="$(prom_query_scalar 'sum(rate(cache_hit_total[5m])) / clamp_min(sum(rate(cache_hit_total[5m])) + sum(rate(cache_miss_total[5m])), 1)')"
prom_pipeline_p95="$(prom_query_scalar 'histogram_quantile(0.95, sum(rate(price_pipeline_end_to_end_seconds_bucket[5m])) by (le))')"
prom_consumer_lag="$(prom_query_scalar 'sum(kafka_consumer_lag)')"
prom_api_cached_p95="$(prom_query_scalar 'histogram_quantile(0.95, sum(rate(latest_price_request_duration_seconds_bucket{cached=\"true\"}[5m])) by (le))')"
prom_api_uncached_p95="$(prom_query_scalar 'histogram_quantile(0.95, sum(rate(latest_price_request_duration_seconds_bucket{cached=\"false\"}[5m])) by (le))')"

cat >"$SUMMARY_JSON" <<EOF
{
  "config": {
    "duration_seconds": ${DURATION_SECONDS},
    "provider": "${PROVIDER}",
    "topic": "${TOPIC}",
    "write_symbols": ${WRITE_SYMBOLS},
    "write_rate_eps": ${WRITE_RATE},
    "read_rps": ${READ_RPS},
    "read_threads": ${READ_THREADS},
    "read_hot_symbols": ${READ_HOT_SYMBOLS}
  },
  "write_load": {
    "events_sent": "${write_events_sent}",
    "errors": "${write_errors}",
    "achieved_throughput_eps": "${write_throughput}"
  },
  "read_load": {
    "total_requests": "${read_total}",
    "success_requests": "${read_success}",
    "error_requests": "${read_errors}",
    "achieved_rps": "${read_achieved_rps}",
    "p95_latency_ms_approx": "${read_p95_ms}",
    "p99_latency_ms_approx": "${read_p99_ms}"
  },
  "prometheus_snapshot": {
    "events_processed_rate_5m": "${prom_events_rate}",
    "cache_hit_ratio_5m": "${prom_cache_ratio}",
    "pipeline_p95_seconds_5m": "${prom_pipeline_p95}",
    "consumer_lag_total": "${prom_consumer_lag}",
    "api_cached_p95_seconds_5m": "${prom_api_cached_p95}",
    "api_uncached_p95_seconds_5m": "${prom_api_uncached_p95}"
  }
}
EOF

cat >"$SUMMARY_MD" <<EOF
# Soak Benchmark Summary

- Duration: ${DURATION_SECONDS}s
- Provider: ${PROVIDER}
- Topic: ${TOPIC}
- Output directory: \`${OUT_DIR}\`

## Write Load

- events_sent: ${write_events_sent}
- errors: ${write_errors}
- achieved_throughput_eps: ${write_throughput}

## Read Load

- total_requests: ${read_total}
- success_requests: ${read_success}
- error_requests: ${read_errors}
- achieved_rps: ${read_achieved_rps}
- p95_latency_ms_approx: ${read_p95_ms}
- p99_latency_ms_approx: ${read_p99_ms}

## Prometheus Snapshot (last 5m)

- events_processed_rate_5m: ${prom_events_rate}
- cache_hit_ratio_5m: ${prom_cache_ratio}
- pipeline_p95_seconds_5m: ${prom_pipeline_p95}
- consumer_lag_total: ${prom_consumer_lag}
- api_cached_p95_seconds_5m: ${prom_api_cached_p95}
- api_uncached_p95_seconds_5m: ${prom_api_uncached_p95}
EOF

log "Done. Summary:"
cat "$SUMMARY_MD"
log "Raw logs: $WRITE_LOG and $READ_LOG"
log "Machine-readable summary: $SUMMARY_JSON"
