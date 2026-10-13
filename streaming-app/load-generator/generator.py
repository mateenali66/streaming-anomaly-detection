"""
Event load generator (e-commerce or IoT workload) for the streaming anomaly
detection benchmark.

Produces JSON-serialized events to Kafka with configurable throughput,
sinusoidal variation, and realistic event distributions.

Error handling strategy:
- Kafka connection: retry with exponential backoff up to 5 min
- Schema Registry: optional (used for contract validation only)
- Producer BufferError: poll and retry (backpressure handling)
- Delivery failures: log and count, continue producing
- Signal handling: SIGTERM/SIGINT trigger graceful flush and shutdown
- Rate limiting: adaptive sleep to match target rate without busy-waiting
"""
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

from confluent_kafka import Producer, KafkaError, KafkaException

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load-generator")

# Graceful shutdown
running = True


def signal_handler(sig, frame):
    global running
    log.info("Received signal %s, initiating graceful shutdown...", sig)
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ---------------------------------------------------------------------------
# Pre-generated pools (deterministic seed for reproducibility)
# ---------------------------------------------------------------------------
_rng = random.Random(42)

if config.WORKLOAD == "ECOMMERCE":
    USER_IDS = [f"user-{i:05d}" for i in range(config.NUM_USERS)]
    PRODUCT_IDS = [f"prod-{i:04d}" for i in range(config.NUM_PRODUCTS)]
    SESSION_IDS = [f"sess-{uuid.UUID(int=_rng.getrandbits(128)).hex[:12]}" for _ in range(config.NUM_SESSIONS)]
    IP_POOL = [f"10.{_rng.randint(0,255)}.{_rng.randint(0,255)}.{_rng.randint(1,254)}" for _ in range(500)]
    PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]
    ACTION_TYPES = ["LOGIN", "LOGOUT", "SEARCH", "FILTER", "WISHLIST_ADD", "REVIEW_SUBMIT"]
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Safari/17.0",
        "Mozilla/5.0 (Linux; Android 14) Mobile Chrome/120.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Mobile Safari/17.0",
    ]
    PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books", "Toys", "Food", "Beauty"]
    PRODUCT_BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF", "BrandG", "BrandH"]
    PRODUCT_CATALOG = {}
    for pid in PRODUCT_IDS:
        PRODUCT_CATALOG[pid] = {
            "category": PRODUCT_CATEGORIES[_rng.randint(0, len(PRODUCT_CATEGORIES) - 1)],
            "brand": PRODUCT_BRANDS[_rng.randint(0, len(PRODUCT_BRANDS) - 1)],
            "base_price": round(10.0 + _rng.random() * 490.0, 2),
        }
elif config.WORKLOAD == "IOT":
    # 100K devices distributed across LOCATION_IDS regions; each device has a fixed home location.
    DEVICE_IDS = [f"dev-{i:06d}" for i in range(config.NUM_DEVICES)]
    LOCATION_IDS = list(range(config.NUM_LOCATIONS))
    DEVICE_LOCATION = {
        did: _rng.randint(0, config.NUM_LOCATIONS - 1)
        for did in DEVICE_IDS
    }
    # Per-device baseline temperature offset to give the location-aggregation Flink job
    # something heterogeneous to summarise (some locations run hotter than others).
    LOCATION_TEMP_OFFSET = {
        loc: _rng.gauss(0.0, 1.5) for loc in LOCATION_IDS
    }
else:
    raise ValueError(f"Unknown WORKLOAD={config.WORKLOAD!r}; expected ECOMMERCE or IOT")


# ---------------------------------------------------------------------------
# Event generators (return key, dict)
# ---------------------------------------------------------------------------
def generate_page_view() -> tuple[str, dict]:
    user_id = random.choice(USER_IDS)
    product_id = random.choice(PRODUCT_IDS)
    return user_id, {
        "event_type": "page_view",
        "event_id": uuid.uuid4().hex,
        "user_id": user_id,
        "product_id": product_id,
        "session_id": random.choice(SESSION_IDS),
        "timestamp": int(time.time() * 1000),
        "page_url": f"/products/{product_id}",
        "referrer": random.choice([None, "/search", "/category/electronics", "/home"]),
        "user_agent": random.choice(USER_AGENTS),
        "ip_address": random.choice(IP_POOL),
    }


def generate_add_to_cart() -> tuple[str, dict]:
    user_id = random.choice(USER_IDS)
    product_id = random.choice(PRODUCT_IDS)
    price = round(random.uniform(5.0, 500.0), 2)
    return user_id, {
        "event_type": "add_to_cart",
        "event_id": uuid.uuid4().hex,
        "user_id": user_id,
        "product_id": product_id,
        "session_id": random.choice(SESSION_IDS),
        "timestamp": int(time.time() * 1000),
        "quantity": random.randint(1, 5),
        "unit_price": price,
        "ip_address": random.choice(IP_POOL),
    }


def generate_purchase() -> tuple[str, dict]:
    user_id = random.choice(USER_IDS)
    num_items = random.randint(1, 5)
    items = []
    total = 0.0
    for _ in range(num_items):
        product_id = random.choice(PRODUCT_IDS)
        price = round(random.uniform(10.0, 300.0), 2)
        qty = random.randint(1, 3)
        total += price * qty
        items.append({
            "product_id": product_id,
            "quantity": qty,
            "unit_price": price,
        })
    return user_id, {
        "event_type": "purchase",
        "event_id": uuid.uuid4().hex,
        "user_id": user_id,
        "order_id": f"order-{uuid.uuid4().hex[:10]}",
        "session_id": random.choice(SESSION_IDS),
        "timestamp": int(time.time() * 1000),
        "items": items,
        "total": round(total, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "ip_address": random.choice(IP_POOL),
    }


def generate_user_action() -> tuple[str, dict]:
    user_id = random.choice(USER_IDS)
    action = random.choice(ACTION_TYPES)
    metadata = {"source": "web"}
    if action == "SEARCH":
        metadata["query"] = random.choice(["laptop", "phone", "headphones", "tablet", "camera"])
    elif action == "FILTER":
        metadata["filter_by"] = random.choice(["price", "rating", "brand"])
    return user_id, {
        "event_type": "user_action",
        "event_id": uuid.uuid4().hex,
        "user_id": user_id,
        "session_id": random.choice(SESSION_IDS),
        "timestamp": int(time.time() * 1000),
        "action_type": action,
        "metadata": metadata,
        "ip_address": random.choice(IP_POOL),
    }


# ---------------------------------------------------------------------------
# IoT event generator
# ---------------------------------------------------------------------------
def generate_iot_event() -> tuple[str, dict]:
    device_id = random.choice(DEVICE_IDS)
    location_id = DEVICE_LOCATION[device_id]
    base_temp = config.IOT_TEMP_MEAN + LOCATION_TEMP_OFFSET[location_id]
    # Rare temperature spikes give the threshold-breach Flink job something to fire on.
    if random.random() < config.IOT_TEMP_SPIKE_PROB:
        temperature = base_temp + random.uniform(20.0, 30.0)
    else:
        temperature = random.gauss(base_temp, config.IOT_TEMP_STD)
    humidity = max(0.0, min(100.0,
                            random.gauss(config.IOT_HUMIDITY_MEAN, config.IOT_HUMIDITY_STD)))
    battery_level = max(0.0, min(100.0,
                                 random.gauss(config.IOT_BATTERY_MEAN, config.IOT_BATTERY_STD)))
    return device_id, {
        "event_type": "iot_sensor",
        "device_id": device_id,
        "timestamp": int(time.time() * 1000),
        "temperature": round(temperature, 3),
        "humidity": round(humidity, 3),
        "battery_level": round(battery_level, 3),
        "location_id": location_id,
    }


# ---------------------------------------------------------------------------
# Workload-conditional generator and weight tables
# ---------------------------------------------------------------------------
if config.WORKLOAD == "ECOMMERCE":
    GENERATORS = {
        "page_view": generate_page_view,
        "add_to_cart": generate_add_to_cart,
        "purchase": generate_purchase,
        "user_action": generate_user_action,
    }
    EVENT_WEIGHTS = config.EVENT_WEIGHTS
elif config.WORKLOAD == "IOT":
    GENERATORS = {"iot_sensor": generate_iot_event}
    EVENT_WEIGHTS = {"iot_sensor": 1.0}
else:
    raise ValueError(f"Unknown WORKLOAD={config.WORKLOAD!r}")

_EVENT_TYPES = list(EVENT_WEIGHTS.keys())
_CUMULATIVE_WEIGHTS = []
_cum = 0.0
for w in EVENT_WEIGHTS.values():
    _cum += w
    _CUMULATIVE_WEIGHTS.append(_cum)


def pick_event_type() -> str:
    if len(_EVENT_TYPES) == 1:
        return _EVENT_TYPES[0]
    r = random.random()
    for i, cw in enumerate(_CUMULATIVE_WEIGHTS):
        if r <= cw:
            return _EVENT_TYPES[i]
    return _EVENT_TYPES[-1]


def get_target_rate(base_rate: int, elapsed_sec: float) -> float:
    """Sinusoidal +/- 20% applies to both workloads. IoT additionally overlays a
    short burst pattern (2x volume for BURST_DURATION_SEC every BURST_PERIOD_SEC)
    to stress Flink rebalance and broker page-cache behaviour."""
    phase = (2 * math.pi * elapsed_sec) / config.SINUSOIDAL_PERIOD_SEC
    multiplier = 1.0 + config.SINUSOIDAL_AMPLITUDE * math.sin(phase)
    if config.WORKLOAD == "IOT":
        cycle = elapsed_sec % config.BURST_PERIOD_SEC
        if cycle < config.BURST_DURATION_SEC:
            multiplier *= config.BURST_MULTIPLIER
    return base_rate * multiplier


# ---------------------------------------------------------------------------
# Kafka producer with retry logic
# ---------------------------------------------------------------------------
def create_producer_with_retry(max_retries: int = 10, base_delay: float = 2.0) -> Producer:
    """Create Kafka producer with exponential backoff retry on connection failure."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = Producer({
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                "acks": config.ACKS,
                "batch.size": config.BATCH_SIZE,
                "linger.ms": config.LINGER_MS,
                "compression.type": config.COMPRESSION,
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
                "retries": 10,
                "retry.backoff.ms": 100,
                "socket.timeout.ms": 10000,
                "metadata.request.timeout.ms": 10000,
                "message.send.max.retries": 5,
            })
            # Verify connectivity by requesting metadata
            metadata = producer.list_topics(timeout=10)
            log.info("Connected to Kafka cluster with %d brokers, %d topics",
                     len(metadata.brokers), len(metadata.topics))
            return producer
        except (KafkaException, Exception) as e:
            delay = min(base_delay * (2 ** (attempt - 1)), 300)
            log.warning("Kafka connection attempt %d/%d failed: %s. Retrying in %.0fs...",
                        attempt, max_retries, e, delay)
            if attempt == max_retries:
                log.error("Failed to connect to Kafka after %d attempts", max_retries)
                raise
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Metrics tracking
# ---------------------------------------------------------------------------
class Metrics:
    def __init__(self):
        self.total_sent = 0
        self.total_errors = 0
        self.total_delivery_failures = 0
        self.by_type = {et: 0 for et in GENERATORS}
        self.batch_sent = 0
        self.batch_errors = 0
        self.batch_start = time.time()

    def record_sent(self, event_type: str):
        self.total_sent += 1
        self.batch_sent += 1
        self.by_type[event_type] += 1

    def record_error(self):
        self.total_errors += 1
        self.batch_errors += 1

    def record_delivery_failure(self):
        self.total_delivery_failures += 1

    def log_and_reset_batch(self, elapsed: float, target_rate: float):
        now = time.time()
        batch_elapsed = now - self.batch_start
        if batch_elapsed <= 0:
            return
        actual_rate = self.batch_sent / batch_elapsed
        by_type_str = " ".join(f"{k}={v}" for k, v in self.by_type.items())
        log.info(
            "[%6.0fs] target=%5.0f/s actual=%5.0f/s total=%d errors=%d dlq_fails=%d | %s",
            elapsed, target_rate, actual_rate,
            self.total_sent, self.total_errors, self.total_delivery_failures,
            by_type_str,
        )
        self.batch_sent = 0
        self.batch_errors = 0
        self.batch_start = now


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run():
    base_rate = config.PROFILES.get(config.THROUGHPUT_PROFILE, 5000)
    log.info("=== Load Generator Configuration ===")
    log.info("  Workload:        %s", config.WORKLOAD)
    log.info("  Kafka:           %s", config.BOOTSTRAP_SERVERS)
    log.info("  Topic:           %s", config.TOPIC)
    log.info("  Profile:         %s (%d events/sec base)", config.THROUGHPUT_PROFILE, base_rate)
    log.info("  Sinusoidal:      +/- %.0f%% over %ds cycle", config.SINUSOIDAL_AMPLITUDE * 100, config.SINUSOIDAL_PERIOD_SEC)
    if config.WORKLOAD == "IOT":
        log.info("  Burst:           %.1fx for %ds every %ds",
                 config.BURST_MULTIPLIER, config.BURST_DURATION_SEC, config.BURST_PERIOD_SEC)
        log.info("  Devices:         %d across %d locations",
                 config.NUM_DEVICES, config.NUM_LOCATIONS)
    log.info("  Event weights:   %s", EVENT_WEIGHTS)
    log.info("  Compression:     %s", config.COMPRESSION)
    log.info("====================================")

    producer = create_producer_with_retry()
    metrics = Metrics()

    def delivery_callback(err, msg):
        if err is not None:
            metrics.record_delivery_failure()
            if metrics.total_delivery_failures % 100 == 1:
                log.warning("Delivery failure #%d: %s", metrics.total_delivery_failures, err)

    start_time = time.time()
    log_interval = 10.0

    while running:
        elapsed = time.time() - start_time
        target_rate = get_target_rate(base_rate, elapsed)
        # Produce in micro-batches: ~100 batches/sec for smooth rate control
        events_per_batch = max(1, int(target_rate / 100))
        batch_loop_start = time.time()

        for _ in range(events_per_batch):
            if not running:
                break

            event_type = pick_event_type()
            key, value = GENERATORS[event_type]()

            try:
                producer.produce(
                    topic=config.TOPIC,
                    key=key.encode("utf-8"),
                    value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
                    headers={"event_type": event_type.encode("utf-8")},
                    callback=delivery_callback,
                )
                metrics.record_sent(event_type)
            except BufferError:
                # Internal producer buffer full: poll to drain and retry once
                producer.poll(0.5)
                try:
                    producer.produce(
                        topic=config.TOPIC,
                        key=key.encode("utf-8"),
                        value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
                        headers={"event_type": event_type.encode("utf-8")},
                        callback=delivery_callback,
                    )
                    metrics.record_sent(event_type)
                except BufferError:
                    metrics.record_error()
                    log.warning("Producer buffer still full after poll, dropping event")
            except KafkaException as e:
                metrics.record_error()
                if metrics.total_errors % 100 == 1:
                    log.error("Kafka produce error #%d: %s", metrics.total_errors, e)
            except Exception as e:
                metrics.record_error()
                log.error("Unexpected error producing %s: %s", event_type, e, exc_info=True)

        # Poll for delivery callbacks without blocking
        producer.poll(0)

        # Adaptive rate limiting
        now = time.time()
        batch_elapsed = now - batch_loop_start
        expected_elapsed = events_per_batch / target_rate if target_rate > 0 else 0
        sleep_time = expected_elapsed - batch_elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

        # Periodic stats logging
        if now - metrics.batch_start >= log_interval:
            metrics.log_and_reset_batch(elapsed, target_rate)

    # Graceful shutdown
    log.info("Flushing producer buffer...")
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        log.warning("%d messages were not delivered during flush", remaining)

    log.info("=== Shutdown Complete ===")
    log.info("  Total sent:              %d", metrics.total_sent)
    log.info("  Total produce errors:    %d", metrics.total_errors)
    log.info("  Total delivery failures: %d", metrics.total_delivery_failures)
    log.info("  By type: %s", {k: v for k, v in metrics.by_type.items()})
    log.info("  Runtime: %.1f seconds", time.time() - start_time)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        log.critical("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
