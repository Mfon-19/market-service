import json
import logging
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server
from pydantic import ValidationError

from app.core.config import settings
from app.db.init_db import init_db
from app.db.session import SessionLocal
from app.schemas.events import PriceEvent
from app.services.metrics import KAFKA_CONSUMER_LAG, KAFKA_EVENTS_CONSUMED_TOTAL
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
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
        except NoBrokersAvailable:
            logger.warning("Kafka broker unavailable, retrying in 5 seconds")
            time.sleep(5)


def update_lag(consumer: KafkaConsumer, topic: str, partition: int, current_offset: int) -> None:
    topic_partition = TopicPartition(topic, partition)
    end_offsets = consumer.end_offsets([topic_partition])
    high_watermark = end_offsets.get(topic_partition, current_offset + 1)
    lag = max(0, high_watermark - current_offset - 1)
    KAFKA_CONSUMER_LAG.labels(topic=topic, partition=str(partition)).set(lag)


def main() -> None:
    init_db()
    start_http_server(settings.worker_metrics_port)
    consumer = create_consumer()
    ma_service = MovingAverageService(window_size=5)

    logger.info("Moving average consumer started")

    try:
        for message in consumer:
            try:
                event = PriceEvent.model_validate(message.value)
            except ValidationError:
                logger.exception("Invalid event payload received: %s", message.value)
                continue

            with SessionLocal() as db:
                ma_service.process_event(db, event)

            KAFKA_EVENTS_CONSUMED_TOTAL.inc()
            update_lag(consumer, message.topic, message.partition, message.offset)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
