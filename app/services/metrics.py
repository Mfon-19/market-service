"""Prometheus metric definitions shared by the API and worker processes."""

from prometheus_client import Counter, Gauge, Histogram


HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency by route/method/status",
    ["route", "method", "status"],
)
LATEST_PRICE_REQUEST_DURATION_SECONDS = Histogram(
    "latest_price_request_duration_seconds",
    "Latency of /prices/latest split by cache hit and status",
    ["cached", "status"],
)

PROVIDER_CALL_LATENCY_SECONDS = Histogram(
    "provider_call_latency_seconds",
    "Latency of provider HTTP calls",
    ["provider"],
)
PROVIDER_CALL_ERRORS_TOTAL = Counter(
    "provider_call_errors_total",
    "Total provider HTTP call errors",
    ["provider"],
)
PROVIDER_RATE_LIMIT_HITS_TOTAL = Counter(
    "provider_rate_limit_hits_total",
    "Total provider rate limiter waits",
    ["provider"],
)
KAFKA_EVENTS_PUBLISHED_TOTAL = Counter(
    "kafka_events_published_total",
    "Total Kafka price events published",
)
EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Total Kafka price events processed by consumer",
)
CONSUMER_PROCESS_DURATION_SECONDS = Histogram(
    "consumer_process_duration_seconds",
    "Time to process one event in the consumer",
)
PRICE_PIPELINE_END_TO_END_SECONDS = Histogram(
    "price_pipeline_end_to_end_seconds",
    "End-to-end pipeline latency from event timestamp to latest_prices update",
)
DB_WRITE_DURATION_SECONDS = Histogram(
    "db_write_duration_seconds",
    "Latency of consumer DB write transaction",
)
DB_QUERY_LATENCY_SECONDS = Histogram(
    "db_query_latency_seconds",
    "Latency of DB query operations",
    ["operation"],
)
CACHE_HIT_TOTAL = Counter(
    "cache_hit_total",
    "Total Redis cache hits",
)
CACHE_MISS_TOTAL = Counter(
    "cache_miss_total",
    "Total Redis cache misses",
)
KAFKA_CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "Approximate Kafka consumer lag",
    ["topic", "partition"],
)

# Backward-compatible aliases for existing imports.
KAFKA_EVENTS_CONSUMED_TOTAL = EVENTS_PROCESSED_TOTAL
CACHE_HITS_TOTAL = CACHE_HIT_TOTAL
CACHE_MISSES_TOTAL = CACHE_MISS_TOTAL
