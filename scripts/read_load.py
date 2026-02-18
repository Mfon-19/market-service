#!/usr/bin/env python3
import argparse
import json
import math
import random
import threading
import time
from collections import Counter
from dataclasses import dataclass, field

import requests

LATENCY_BUCKETS_MS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]


@dataclass
class WorkerStats:
    total_requests: int = 0
    success_requests: int = 0
    error_requests: int = 0
    cache_true_responses: int = 0
    cache_false_responses: int = 0
    latency_sum_ms: float = 0.0
    latency_max_ms: float = 0.0
    status_counts: Counter = field(default_factory=Counter)
    latency_buckets: list[int] = field(default_factory=lambda: [0] * (len(LATENCY_BUCKETS_MS) + 1))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HTTP read-load generator for /prices/latest")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--provider", default="bench")
    parser.add_argument("--duration", type=int, default=3600, help="Run duration in seconds")
    parser.add_argument("--total-rps", type=float, default=500.0, help="Target aggregate requests/sec")
    parser.add_argument("--threads", type=int, default=50)
    parser.add_argument("--symbol-pool", type=int, default=2000, help="Total symbol cardinality")
    parser.add_argument("--hot-symbols", type=int, default=200, help="Hot set size (from start of pool)")
    parser.add_argument("--hot-ratio", type=float, default=0.9, help="Probability of choosing hot set")
    parser.add_argument("--symbol-prefix", default="SYM")
    parser.add_argument("--symbol-width", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=2.0)
    parser.add_argument("--output", default="", help="Optional path to write JSON summary")
    return parser.parse_args()


def latency_bucket_index(latency_ms: float) -> int:
    for i, bound in enumerate(LATENCY_BUCKETS_MS):
        if latency_ms <= bound:
            return i
    return len(LATENCY_BUCKETS_MS)


def select_symbol(
    rng: random.Random,
    symbol_pool: int,
    hot_symbols: int,
    hot_ratio: float,
    symbol_prefix: str,
    symbol_width: int,
) -> str:
    if symbol_pool <= hot_symbols or rng.random() < hot_ratio:
        index = rng.randrange(max(1, hot_symbols))
    else:
        index = hot_symbols + rng.randrange(max(1, symbol_pool - hot_symbols))
    return f"{symbol_prefix}{index:0{symbol_width}d}"


def merge_stats(dest: WorkerStats, src: WorkerStats) -> None:
    dest.total_requests += src.total_requests
    dest.success_requests += src.success_requests
    dest.error_requests += src.error_requests
    dest.cache_true_responses += src.cache_true_responses
    dest.cache_false_responses += src.cache_false_responses
    dest.latency_sum_ms += src.latency_sum_ms
    dest.latency_max_ms = max(dest.latency_max_ms, src.latency_max_ms)
    dest.status_counts.update(src.status_counts)
    for i, count in enumerate(src.latency_buckets):
        dest.latency_buckets[i] += count


def approximate_quantile_from_histogram(hist: list[int], q: float) -> float:
    total = sum(hist)
    if total == 0:
        return math.nan
    threshold = max(1, math.ceil(total * q))
    running = 0
    for i, count in enumerate(hist):
        running += count
        if running >= threshold:
            if i < len(LATENCY_BUCKETS_MS):
                return float(LATENCY_BUCKETS_MS[i])
            return float("inf")
    return float("inf")


def main() -> None:
    args = parse_args()
    total_threads = max(1, args.threads)
    symbol_pool = max(1, args.symbol_pool)
    hot_symbols = max(1, min(args.hot_symbols, symbol_pool))
    per_thread_rps = max(0.001, args.total_rps / total_threads)
    endpoint = f"{args.base_url.rstrip('/')}/prices/latest"

    start = time.time()
    deadline = start + max(1, args.duration)
    workers: list[WorkerStats | None] = [None] * total_threads

    def worker(idx: int) -> None:
        rng = random.Random(idx + int(start))
        session = requests.Session()
        local = WorkerStats()
        next_send = time.perf_counter()
        while time.time() < deadline:
            symbol = select_symbol(
                rng=rng,
                symbol_pool=symbol_pool,
                hot_symbols=hot_symbols,
                hot_ratio=args.hot_ratio,
                symbol_prefix=args.symbol_prefix,
                symbol_width=args.symbol_width,
            )
            t0 = time.perf_counter()
            try:
                response = session.get(
                    endpoint,
                    params={"symbol": symbol, "provider": args.provider},
                    timeout=args.timeout_seconds,
                )
                latency_ms = (time.perf_counter() - t0) * 1000.0
                local.total_requests += 1
                local.latency_sum_ms += latency_ms
                local.latency_max_ms = max(local.latency_max_ms, latency_ms)
                local.latency_buckets[latency_bucket_index(latency_ms)] += 1
                local.status_counts[str(response.status_code)] += 1

                if 200 <= response.status_code < 300:
                    local.success_requests += 1
                    try:
                        payload = response.json()
                        if payload.get("cached") is True:
                            local.cache_true_responses += 1
                        else:
                            local.cache_false_responses += 1
                    except Exception:
                        pass
                else:
                    local.error_requests += 1
            except Exception:
                latency_ms = (time.perf_counter() - t0) * 1000.0
                local.total_requests += 1
                local.error_requests += 1
                local.latency_sum_ms += latency_ms
                local.latency_max_ms = max(local.latency_max_ms, latency_ms)
                local.latency_buckets[latency_bucket_index(latency_ms)] += 1
                local.status_counts["exception"] += 1

            next_send += 1.0 / per_thread_rps
            sleep_for = next_send - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

        workers[idx] = local

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(total_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    elapsed = max(0.001, time.time() - start)
    combined = WorkerStats()
    for local in workers:
        if local is not None:
            merge_stats(combined, local)

    avg_latency_ms = combined.latency_sum_ms / max(1, combined.total_requests)
    result = {
        "target_rps": args.total_rps,
        "achieved_rps": combined.total_requests / elapsed,
        "duration_seconds": elapsed,
        "total_requests": combined.total_requests,
        "success_requests": combined.success_requests,
        "error_requests": combined.error_requests,
        "status_counts": dict(combined.status_counts),
        "cache_true_responses": combined.cache_true_responses,
        "cache_false_responses": combined.cache_false_responses,
        "latency_ms": {
            "avg": avg_latency_ms,
            "max": combined.latency_max_ms,
            "p50_approx": approximate_quantile_from_histogram(combined.latency_buckets, 0.50),
            "p95_approx": approximate_quantile_from_histogram(combined.latency_buckets, 0.95),
            "p99_approx": approximate_quantile_from_histogram(combined.latency_buckets, 0.99),
        },
        "latency_buckets_ms_upper_bounds": LATENCY_BUCKETS_MS + ["+Inf"],
        "latency_buckets_counts": combined.latency_buckets,
    }

    serialized = json.dumps(result, indent=2, sort_keys=True)
    print(serialized)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(serialized)
            f.write("\n")


if __name__ == "__main__":
    main()
