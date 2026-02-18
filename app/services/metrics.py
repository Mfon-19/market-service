from prometheus_client import Counter, Gauge, Histogram


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
KAFKA_EVENTS_CONSUMED_TOTAL = Counter(
    "kafka_events_consumed_total",
    "Total Kafka price events consumed",
)
DB_QUERY_LATENCY_SECONDS = Histogram(
    "db_query_latency_seconds",
    "Latency of DB query operations",
    ["operation"],
)
CACHE_HITS_TOTAL = Counter(
    "cache_hits_total",
    "Total Redis cache hits",
)
CACHE_MISSES_TOTAL = Counter(
    "cache_misses_total",
    "Total Redis cache misses",
)
KAFKA_CONSUMER_LAG = Gauge(
    "kafka_consumer_lag",
    "Approximate Kafka consumer lag",
    ["topic", "partition"],
)
