import json
import logging

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.errors import TopicAlreadyExistsError

from app.core.config import Settings
from app.schemas.events import PriceEvent
from app.services.metrics import KAFKA_EVENTS_PUBLISHED_TOTAL

logger = logging.getLogger(__name__)


class PriceEventProducer:
    def __init__(self, settings: Settings) -> None:
        self.topic = settings.kafka_topic
        self.topic_partitions = max(1, settings.kafka_topic_partitions)
        self.enabled = True

        try:
            self._ensure_topic(settings)
            self._producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
                key_serializer=lambda key: key,
                retries=3,
            )
        except NoBrokersAvailable:
            logger.warning("Kafka not available at startup, producer disabled")
            self._producer = None
            self.enabled = False

    def _ensure_topic(self, settings: Settings) -> None:
        try:
            admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
        except NoBrokersAvailable:
            return

        topic = NewTopic(name=self.topic, num_partitions=self.topic_partitions, replication_factor=1)
        try:
            admin.create_topics(new_topics=[topic], validate_only=False)
            logger.info("Created Kafka topic %s with %s partitions", self.topic, self.topic_partitions)
        except TopicAlreadyExistsError:
            pass
        except KafkaError:
            logger.warning("Unable to ensure Kafka topic %s exists", self.topic)

        try:
            topic_metadata = admin.describe_topics([self.topic])
            existing_partitions = len(topic_metadata[0].get("partitions", []))
            if 0 < existing_partitions < self.topic_partitions:
                admin.create_partitions(
                    topic_partitions={
                        self.topic: NewPartitions(total_count=self.topic_partitions),
                    },
                    validate_only=False,
                )
                logger.info(
                    "Increased Kafka topic %s partitions from %s to %s",
                    self.topic,
                    existing_partitions,
                    self.topic_partitions,
                )
        except KafkaError:
            logger.warning("Unable to ensure partition count for topic %s", self.topic)
        finally:
            admin.close()

    def send_event(self, event: PriceEvent) -> None:
        if not self.enabled or self._producer is None:
            return

        payload = event.model_dump(mode="json")
        message_key = f"{event.provider}:{event.symbol}".encode("utf-8")
        try:
            future = self._producer.send(self.topic, key=message_key, value=payload)
            future.get(timeout=10)
            KAFKA_EVENTS_PUBLISHED_TOTAL.inc()
        except KafkaError:
            logger.exception("Failed to publish Kafka message")

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=5)
            self._producer.close()
