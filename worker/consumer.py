import json
import logging
import time
from collections.abc import Iterable

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server
from pydantic import ValidationError

from app.core.config import settings
from app.db.init_db import init_db
from app.db.session import SessionLocal
from app.schemas.events import PriceEvent
from app.services.metrics import KAFKA_CONSUMER_LAG
from app.services.moving_average import MovingAverageService

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                max_poll_records=settings.consumer_batch_size,
            )
        except NoBrokersAvailable:
            logger.warning("Kafka broker unavailable, retrying in 5 seconds")
            time.sleep(5)


def flatten_records(records_map: dict) -> list:
    ordered_records: list = []
    for records in records_map.values():
        ordered_records.extend(records)
    return ordered_records


def parse_events(messages: Iterable) -> list[PriceEvent]:
    events: list[PriceEvent] = []
    for message in messages:
        try:
            events.append(PriceEvent.model_validate(message.value))
        except ValidationError:
            logger.exception("Invalid event payload received: %s", message.value)
    return events


def update_lag(consumer: KafkaConsumer, records_map: dict) -> None:
    if not records_map:
        return

    partitions = list(records_map.keys())
    end_offsets = consumer.end_offsets(partitions)
    for partition, records in records_map.items():
        if not records:
            continue
        last_offset = records[-1].offset
        high_watermark = end_offsets.get(partition, last_offset + 1)
        lag = max(0, high_watermark - last_offset - 1)
        KAFKA_CONSUMER_LAG.labels(topic=partition.topic, partition=str(partition.partition)).set(lag)


def main() -> None:
    init_db()
    start_http_server(settings.worker_metrics_port)
    consumer = create_consumer()
    ma_service = MovingAverageService(window_size=5)

    logger.info(
        "Moving average consumer started (batch_size=%s poll_timeout_ms=%s)",
        settings.consumer_batch_size,
        settings.consumer_poll_timeout_ms,
    )

    try:
        while True:
            records_map = consumer.poll(
                timeout_ms=settings.consumer_poll_timeout_ms,
                max_records=settings.consumer_batch_size,
            )
            if not records_map:
                continue

            messages = flatten_records(records_map)
            events = parse_events(messages)
            if not events:
                consumer.commit()
                update_lag(consumer, records_map)
                continue

            try:
                with SessionLocal() as db:
                    ma_service.process_batch(db, events)
                consumer.commit()
                update_lag(consumer, records_map)
            except Exception:
                logger.exception("Consumer batch failed; offsets not committed, retrying")
                time.sleep(settings.consumer_retry_backoff_seconds)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
