#!/usr/bin/env python3
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from threading import Lock

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic Kafka price-event load generator")
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--topic", default="price-events")
    parser.add_argument("--provider", default="bench")
    parser.add_argument("--symbols", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=5000, help="target events/sec")
    parser.add_argument("--duration", type=int, default=30, help="test duration seconds")
    parser.add_argument("--price-base", type=float, default=100.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [f"SYM{i:05d}" for i in range(args.symbols)]
    interval = 1.0 / max(1, args.rate)

    counters = {
        "sent": 0,
        "errors": 0,
    }
    lock = Lock()

    def on_error(exc: Exception) -> None:
        with lock:
            counters["errors"] += 1
        print(f"send_error={exc}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key,
        linger_ms=5,
        batch_size=65536,
        retries=3,
    )

    print(
        f"Starting synthetic load: topic={args.topic} provider={args.provider} "
        f"symbols={args.symbols} rate={args.rate}/s duration={args.duration}s"
    )

    start = time.perf_counter()
    deadline = start + args.duration
    next_send = start
    i = 0

    while True:
        now = time.perf_counter()
        if now >= deadline:
            break

        symbol = symbols[i % len(symbols)]
        price = round(args.price_base + random.random() * 10, 6)
        event_ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "event_id": str(uuid.uuid4()),
            "symbol": symbol,
            "provider": args.provider,
            "price": str(price),
            "timestamp": event_ts,
            "raw_response_id": str(uuid.uuid4()),
            "price_point_id": str(uuid.uuid4()),
        }
        key = f"{args.provider}:{symbol}".encode("utf-8")

        producer.send(args.topic, key=key, value=payload).add_errback(on_error)
        with lock:
            counters["sent"] += 1

        i += 1
        next_send += interval
        sleep_for = next_send - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)

    producer.flush(timeout=30)
    producer.close()

    elapsed = max(0.001, time.perf_counter() - start)
    sent = counters["sent"]
    errors = counters["errors"]
    throughput = sent / elapsed

    print("\nLoad run complete")
    print(f"events_sent={sent}")
    print(f"errors={errors}")
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"achieved_throughput_eps={throughput:.2f}")


if __name__ == "__main__":
    main()
