import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from app.core.config import Settings
from app.schemas.events import PriceEvent
from app.services.metrics import KAFKA_EVENTS_PUBLISHED_TOTAL

logger = logging.getLogger(__name__)


class PriceEventProducer:
    def __init__(self, settings: Settings) -> None:
        self.topic = settings.kafka_topic
        self.enabled = True

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
                retries=3,
            )
        except NoBrokersAvailable:
            logger.warning("Kafka not available at startup, producer disabled")
            self._producer = None
            self.enabled = False

    def send_event(self, event: PriceEvent) -> None:
        if not self.enabled or self._producer is None:
            return

        payload = event.model_dump(mode="json")
        try:
            future = self._producer.send(self.topic, payload)
            future.get(timeout=10)
            KAFKA_EVENTS_PUBLISHED_TOTAL.inc()
        except KafkaError:
            logger.exception("Failed to publish Kafka message")

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=5)
            self._producer.close()
