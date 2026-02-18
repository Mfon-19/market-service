#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
PROVIDER="${PROVIDER:-yahoo}"
PRIMARY_SYMBOL="${PRIMARY_SYMBOL:-AAPL}"
MA_SYMBOL="${MA_SYMBOL:-MSFT}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-market-postgres}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-market-kafka}"
MIN_INTERVAL="${MIN_INTERVAL:-60}"
JOB_WAIT_SECONDS="${JOB_WAIT_SECONDS:-60}"
LOW_TRAFFIC_MODE="${LOW_TRAFFIC_MODE:-false}"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0
SKIP_COUNT=0

HTTP_STATUS=""
HTTP_BODY=""
PROVIDER_AVAILABLE=1

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "PASS: $1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "FAIL: $1"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  echo "WARN: $1"
}

skip() {
  SKIP_COUNT=$((SKIP_COUNT + 1))
  echo "SKIP: $1"
}

step() {
  echo
  echo "== $1 =="
}

http_call() {
  local method="$1"
  local url="$2"
  local data="${3:-}"
  local response

  if [[ -n "$data" ]]; then
    response="$(curl -sS -X "$method" -H "Content-Type: application/json" -d "$data" -w $'\n%{http_code}' "$url")"
  else
    response="$(curl -sS -X "$method" -w $'\n%{http_code}' "$url")"
  fi

  HTTP_STATUS="$(printf '%s' "$response" | tail -n1)"
  HTTP_BODY="$(printf '%s' "$response" | sed '$d')"
}

run_sql() {
  local sql="$1"
  docker exec "$POSTGRES_CONTAINER" psql -U postgres -d market_data -tAc "$sql" | tr -d '\r'
}

require_cmd curl
require_cmd jq
require_cmd docker

step "Health and metrics"
http_call GET "$API_URL/health"
if [[ "$HTTP_STATUS" == "200" ]] && jq -e '.status == "ok"' >/dev/null <<<"$HTTP_BODY"; then
  pass "GET /health is healthy"
else
  fail "GET /health failed (status=$HTTP_STATUS body=$HTTP_BODY)"
fi

METRICS="$(curl -sS "$API_URL/metrics" || true)"
if grep -q "provider_call_latency_seconds" <<<"$METRICS"; then
  pass "GET /metrics exposes custom metrics"
else
  fail "GET /metrics missing expected metric names"
fi

step "Latest price and cache behavior"
http_call GET "$API_URL/prices/latest?symbol=$PRIMARY_SYMBOL&provider=$PROVIDER"
if [[ "$HTTP_STATUS" == "200" ]] && jq -e '.symbol and .provider and .price and .timestamp' >/dev/null <<<"$HTTP_BODY"; then
  pass "First latest-price request returned valid payload"
elif [[ "$HTTP_STATUS" == "502" ]] && jq -e '.detail == "provider request failed"' >/dev/null <<<"$HTTP_BODY"; then
  PROVIDER_AVAILABLE=0
  warn "Provider is unreachable from the API container; skipping provider-dependent checks"
elif [[ "$HTTP_STATUS" == "429" ]]; then
  PROVIDER_AVAILABLE=0
  warn "Provider rate-limited request; skipping provider-dependent checks"
else
  fail "First latest-price request failed (status=$HTTP_STATUS body=$HTTP_BODY)"
fi

if [[ "$LOW_TRAFFIC_MODE" == "true" ]]; then
  skip "Second latest-price cache check skipped in LOW_TRAFFIC_MODE"
elif (( PROVIDER_AVAILABLE == 1 )); then
  http_call GET "$API_URL/prices/latest?symbol=$PRIMARY_SYMBOL&provider=$PROVIDER"
  if [[ "$HTTP_STATUS" != "200" ]]; then
    fail "Second latest-price request failed (status=$HTTP_STATUS body=$HTTP_BODY)"
  else
    CACHED_VALUE="$(jq -r '.cached' <<<"$HTTP_BODY")"
    if [[ "$CACHED_VALUE" == "true" ]]; then
      pass "Second latest-price request hit Redis cache"
    else
      warn "Second latest-price request was not cached (cached=$CACHED_VALUE)"
    fi
  fi
else
  skip "Second latest-price cache check skipped due to provider outage"
fi

step "Postgres write checks"
if (( PROVIDER_AVAILABLE == 1 )); then
  RAW_COUNT="$(run_sql "select count(*) from raw_market_data where symbol='${PRIMARY_SYMBOL}' and provider='${PROVIDER}';" | tr -d '[:space:]')"
  PRICE_COUNT="$(run_sql "select count(*) from price_points where symbol='${PRIMARY_SYMBOL}' and provider='${PROVIDER}';" | tr -d '[:space:]')"

  if [[ "${RAW_COUNT:-0}" =~ ^[0-9]+$ ]] && (( RAW_COUNT > 0 )); then
    pass "raw_market_data has rows for ${PRIMARY_SYMBOL}/${PROVIDER}"
  else
    fail "raw_market_data has no rows for ${PRIMARY_SYMBOL}/${PROVIDER}"
  fi

  if [[ "${PRICE_COUNT:-0}" =~ ^[0-9]+$ ]] && (( PRICE_COUNT > 0 )); then
    pass "price_points has rows for ${PRIMARY_SYMBOL}/${PROVIDER}"
  else
    fail "price_points has no rows for ${PRIMARY_SYMBOL}/${PROVIDER}"
  fi
else
  skip "Postgres ingestion-row checks skipped due to provider outage"
fi

step "Kafka event visibility"
if [[ "$LOW_TRAFFIC_MODE" == "true" ]]; then
  skip "Kafka producer-event check skipped in LOW_TRAFFIC_MODE"
elif (( PROVIDER_AVAILABLE == 1 )); then
  KAFKA_OUTPUT="$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic price-events \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 10000 2>/dev/null || true)"
  KAFKA_LINE="$(printf '%s\n' "$KAFKA_OUTPUT" | tail -n1)"

  if [[ -n "$KAFKA_LINE" ]] && jq -e '.symbol and .provider and .price and .timestamp' >/dev/null <<<"$KAFKA_LINE"; then
    pass "Kafka topic price-events contains valid messages"
  else
    fail "Could not read a valid message from Kafka topic price-events"
  fi
else
  skip "Kafka producer-event check skipped due to provider outage"
fi

step "Moving average computation"
if [[ "$LOW_TRAFFIC_MODE" == "true" ]]; then
  skip "Moving-average check skipped in LOW_TRAFFIC_MODE"
elif (( PROVIDER_AVAILABLE == 1 )); then
  for _ in {1..5}; do
    curl -sS "$API_URL/prices/latest?symbol=$MA_SYMBOL&provider=$PROVIDER" >/dev/null
    sleep 1
  done
  sleep 3

  AVG_ROW="$(run_sql "select sample_size, moving_average from symbol_averages where symbol='${MA_SYMBOL}' and provider='${PROVIDER}' and window_size=5 order by updated_at desc limit 1;" | tr -d '[:space:]')"
  AVG_SAMPLE_SIZE="$(cut -d'|' -f1 <<<"$AVG_ROW")"
  AVG_VALUE="$(cut -d'|' -f2 <<<"$AVG_ROW")"

  if [[ "$AVG_SAMPLE_SIZE" =~ ^[0-9]+$ ]] && (( AVG_SAMPLE_SIZE >= 5 )) && [[ -n "$AVG_VALUE" ]]; then
    pass "5-point moving average exists for ${MA_SYMBOL}/${PROVIDER}"
  else
    fail "5-point moving average not found for ${MA_SYMBOL}/${PROVIDER}"
  fi
else
  skip "Moving-average computation check skipped due to provider outage"
fi

step "Polling job API"
POLL_PAYLOAD="{\"symbols\":[\"${PRIMARY_SYMBOL}\"],\"interval\":${MIN_INTERVAL},\"provider\":\"${PROVIDER}\"}"
http_call POST "$API_URL/prices/poll" "$POLL_PAYLOAD"

if [[ "$HTTP_STATUS" != "202" ]]; then
  fail "POST /prices/poll failed (status=$HTTP_STATUS body=$HTTP_BODY)"
  JOB_ID=""
else
  JOB_ID="$(jq -r '.job_id // empty' <<<"$HTTP_BODY")"
  if [[ -n "$JOB_ID" ]]; then
    pass "POST /prices/poll returned job_id=$JOB_ID"
  else
    fail "POST /prices/poll response missing job_id"
  fi
fi

if [[ -n "${JOB_ID:-}" ]]; then
  LAST_RUN_AT=""
  ATTEMPTS=$(( JOB_WAIT_SECONDS / 3 ))
  if (( ATTEMPTS < 1 )); then
    ATTEMPTS=1
  fi

  for _ in $(seq 1 "$ATTEMPTS"); do
    sleep 3
    http_call GET "$API_URL/prices/poll/$JOB_ID"
    if [[ "$HTTP_STATUS" == "200" ]]; then
      LAST_RUN_AT="$(jq -r '.last_run_at // empty' <<<"$HTTP_BODY")"
      if [[ -n "$LAST_RUN_AT" && "$LAST_RUN_AT" != "null" ]]; then
        break
      fi
    fi
  done

  if [[ -n "$LAST_RUN_AT" && "$LAST_RUN_AT" != "null" ]]; then
    pass "Polling job executed (last_run_at=$LAST_RUN_AT)"
  else
    fail "Polling job did not execute within ${JOB_WAIT_SECONDS}s"
  fi
fi

step "Negative-path checks"
http_call GET "$API_URL/prices/latest?symbol=$PRIMARY_SYMBOL&provider=not_a_real_provider"
if [[ "$HTTP_STATUS" == "400" ]]; then
  pass "Invalid provider returns HTTP 400"
else
  fail "Invalid provider expected HTTP 400, got $HTTP_STATUS"
fi

http_call POST "$API_URL/prices/poll" "{\"symbols\":[\"${PRIMARY_SYMBOL}\"],\"interval\":5,\"provider\":\"${PROVIDER}\"}"
if [[ "$HTTP_STATUS" == "400" ]]; then
  pass "Too-small polling interval returns HTTP 400"
else
  fail "Too-small interval expected HTTP 400, got $HTTP_STATUS"
fi

echo
echo "==== Summary ===="
echo "PASS: $PASS_COUNT"
echo "SKIP: $SKIP_COUNT"
echo "WARN: $WARN_COUNT"
echo "FAIL: $FAIL_COUNT"

if (( FAIL_COUNT > 0 )); then
  exit 1
fi
