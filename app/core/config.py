from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "market-data-service"
    environment: str = "dev"
    log_level: str = "INFO"

    database_url: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/market_data"

    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "price-events"
    kafka_group_id: str = "moving-average-consumer"
    kafka_topic_partitions: int = 8

    redis_url: str = "redis://localhost:6379/0"
    use_redis_cache: bool = True
    cache_ttl_seconds: int = 15

    default_provider: str = "yahoo"
    provider_timeout_seconds: int = 10
    provider_rate_limit_per_minute: int = 2
    provider_http_max_retries: int = 2
    provider_http_backoff_seconds: float = 1.5
    enable_yahoo_rate_limit_fallback: bool = True
    yahoo_rate_limit_fallback_provider: str = "alpha_vantage"
    alpha_vantage_api_key: str = ""
    finnhub_api_key: str = ""

    poll_default_interval_seconds: int = 60
    poll_min_interval_seconds: int = 15
    poll_max_symbols_per_job: int = 20

    consumer_batch_size: int = 200
    consumer_poll_timeout_ms: int = 1000
    consumer_retry_backoff_seconds: float = 2.0

    worker_metrics_port: int = 9108

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
