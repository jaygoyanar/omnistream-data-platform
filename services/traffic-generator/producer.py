import time
import json
import random
import uuid
import multiprocessing
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"  # Points to the "EXTERNAL" listener in Docker
TOPIC_IMPRESSIONS = "ad_impressions"
TOPIC_CLICKS = "ad_clicks"
NUM_PROCESSES = 4  # Number of parallel "users" blasting data

# Simulation Settings
CLICK_PROBABILITY = 0.05  # 5% CTR (Click Through Rate)
CAMPAIGNS = [f"camp-{i}" for i in range(100, 110)]
DEVICES = ["mobile", "desktop", "tablet"]
LOCATIONS = ["US-NY", "US-CA", "IN-DL", "IN-MH", "GB-LND"]
USER_IDS_FILE = "user_ids.json"


# --------------------------------------------------------------------------
# HELPERS
# --------------------------------------------------------------------------
def get_producer():
    """Create a high-performance Kafka Producer instance."""
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "queue.buffering.max.messages": 100000,  # Batch size for speed
        "queue.buffering.max.ms": 500,  # Latency buffer
    }
    return Producer(conf)


def delivery_report(err, msg):
    """Callback for delivery results (optional, can slow down high-throughput)."""
    if err is not None:
        print(f"Delivery failed: {err}")


# --------------------------------------------------------------------------
# THE CORE GENERATOR LOGIC
# --------------------------------------------------------------------------
def generate_traffic(process_id):
    """
    This function runs in its own Process.
    It has its own Memory, Faker instance, and Producer connection.
    """
    print(f"[Process {process_id}] Started.")

    producer = get_producer()
    faker = Faker()

    # Pre-generate some fake User IDs to rotate through (Simulating active users)
    # In Sprint 2, we will fetch these from Postgres!
    # active_users = [f"u-{faker.random_int(1000, 99999)}" for _ in range(1000)
    try:
        if not os.path.exists(USER_IDS_FILE):
            raise FileNotFoundError(
                f"Could not find {USER_IDS_FILE}. Did you run the Seeder script?"
            )

        with open(USER_IDS_FILE, "r") as f:
            active_users = json.load(f)

        print(f"✅ Loaded {len(active_users)} active users.")

    except Exception as e:
        print(f"❌ Error loading users: {e}")
        return []

    count = 0

    try:
        while True:
            # ------------------------------------------
            # 1. Generate Impression
            # ------------------------------------------
            impression_id = str(uuid.uuid4())
            user_id = random.choice(active_users)
            timestamp_now = datetime.now(timezone.utc).isoformat()

            impression = {
                "impression_id": impression_id,
                "user_id": user_id,
                "ad_campaign_id": random.choice(CAMPAIGNS),
                "timestamp": timestamp_now,
                "geo_location": random.choice(LOCATIONS),
                "device_type": random.choice(DEVICES),
                "bid_price": round(random.uniform(0.01, 1.00), 2),
            }

            # Send to Kafka (Async)
            producer.produce(
                TOPIC_IMPRESSIONS,
                key=impression_id,  # Key ensures ordering by Impression ID if needed
                value=json.dumps(impression),
                on_delivery=delivery_report if count % 1000 == 0 else None,
            )

            # ------------------------------------------
            # 2. Probabilistic Click Generation
            # ------------------------------------------
            if random.random() < CLICK_PROBABILITY:
                click_id = str(uuid.uuid4())

                # Simulate a slight delay for the click (humans aren't instant)
                # Note: In a real "flood" test, we might skip the sleep to maximize throughput
                # but let's keep the timestamp logical.

                click = {
                    "click_id": click_id,
                    "impression_id": impression_id,  # LINKED!
                    "user_id": user_id,
                    "timestamp": timestamp_now,  # Keeping simple for high-speed simulation
                    "click_cost": round(
                        impression["bid_price"] * random.uniform(1.2, 5.0), 2
                    ),
                }

                producer.produce(
                    TOPIC_CLICKS,
                    key=impression_id,  # Partition by Impression ID so they land on same node
                    value=json.dumps(click),
                )

            # ------------------------------------------
            # 3. Batch Control
            # ------------------------------------------
            producer.poll(0)  # Trigger callbacks
            count += 1

            # Optional: Print status every 5000 events per process
            if count % 5000 == 0:
                print(f"[Process {process_id}] Sent {count} events...")
                producer.flush()  # Force send buffer

    except KeyboardInterrupt:
        print(f"[Process {process_id}] Stopping...")
        producer.flush()


# --------------------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Starting {NUM_PROCESSES} parallel traffic generators...")
    print(f"Targeting Kafka at: {KAFKA_BROKER}")

    processes = []

    for i in range(NUM_PROCESSES):
        p = multiprocessing.Process(target=generate_traffic, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
