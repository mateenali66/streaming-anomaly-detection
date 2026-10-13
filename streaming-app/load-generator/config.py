import os

# Workload selector: chooses event schema and rate profile.
# ECOMMERCE (default) produces the e-commerce event stream.
# IOT produces the IoT sensor stream used for the cross-workload comparison.
WORKLOAD = os.getenv("WORKLOAD", "ECOMMERCE").upper()

# Kafka
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8080/apis/ccompat/v7")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")

# Throughput profiles (events/second base rate)
THROUGHPUT_PROFILE = os.getenv("THROUGHPUT_PROFILE", "MEDIUM")
PROFILES = {
    # E-commerce profiles
    "ULTRA_LOW": 500,
    "LOW": 1000,
    "MEDIUM": 5000,
    "HIGH": 15000,
    # IoT profiles
    "IOT_LOW": 5000,
    "IOT_MEDIUM": 10000,
    "IOT_HIGH": 20000,
}

# Sinusoidal variation: +/- 20% over 30-min cycle (applies to both workloads)
SINUSOIDAL_AMPLITUDE = float(os.getenv("SINUSOIDAL_AMPLITUDE", "0.2"))
SINUSOIDAL_PERIOD_SEC = int(os.getenv("SINUSOIDAL_PERIOD_SEC", "1800"))

# IoT burst pattern: 2x volume for 30s every 5min (defaults; only used when WORKLOAD=IOT)
BURST_PERIOD_SEC = int(os.getenv("BURST_PERIOD_SEC", "300"))
BURST_DURATION_SEC = int(os.getenv("BURST_DURATION_SEC", "30"))
BURST_MULTIPLIER = float(os.getenv("BURST_MULTIPLIER", "2.0"))

# E-commerce event type distribution (used when WORKLOAD=ECOMMERCE)
EVENT_WEIGHTS = {
    "page_view": 0.50,
    "add_to_cart": 0.25,
    "purchase": 0.15,
    "user_action": 0.10,
}

# E-commerce simulation pool sizes
NUM_USERS = int(os.getenv("NUM_USERS", "10000"))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "500"))
NUM_SESSIONS = int(os.getenv("NUM_SESSIONS", "2000"))

# IoT simulation pool sizes (used when WORKLOAD=IOT)
NUM_DEVICES = int(os.getenv("NUM_DEVICES", "100000"))
NUM_LOCATIONS = int(os.getenv("NUM_LOCATIONS", "500"))
IOT_TEMP_MEAN = float(os.getenv("IOT_TEMP_MEAN", "22.0"))
IOT_TEMP_STD = float(os.getenv("IOT_TEMP_STD", "3.0"))
IOT_HUMIDITY_MEAN = float(os.getenv("IOT_HUMIDITY_MEAN", "55.0"))
IOT_HUMIDITY_STD = float(os.getenv("IOT_HUMIDITY_STD", "10.0"))
IOT_BATTERY_MEAN = float(os.getenv("IOT_BATTERY_MEAN", "75.0"))
IOT_BATTERY_STD = float(os.getenv("IOT_BATTERY_STD", "15.0"))
IOT_TEMP_SPIKE_PROB = float(os.getenv("IOT_TEMP_SPIKE_PROB", "0.001"))  # 0.1% chance per event

# Producer tuning
BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "65536"))
LINGER_MS = int(os.getenv("PRODUCER_LINGER_MS", "5"))
ACKS = os.getenv("PRODUCER_ACKS", "all")
COMPRESSION = os.getenv("PRODUCER_COMPRESSION", "lz4")
