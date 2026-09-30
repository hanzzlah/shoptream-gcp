import json
import os
from datetime import datetime
from dotenv import load_dotenv  # type:ignore
from google.cloud import pubsub_v1  # type:ignore

load_dotenv()

# Support both GCP_PROJECT_ID and PROJECT_ID
PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")
FILE_PATH = os.getenv("JSONL_FILE_PATH", "data/orders_events.jsonl")

# Schema contract required by Dataflow
REQUIRED_FIELDS = {
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_status",
    "region",
    "event_timestamp",
}
VALID_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED"}


def validate_record(data: dict, line_num: int):
    """Pre-checks payload locally to ensure Dataflow won't throw unhandled exceptions."""
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Line {line_num}: Missing required fields: {missing}")

    if int(data["quantity"]) <= 0:
        raise ValueError(f"Line {line_num}: Invalid quantity ({data['quantity']})")

    if data.get("order_status") not in VALID_STATUSES:
        raise ValueError(
            f"Line {line_num}: Invalid status '{data.get('order_status')}'"
        )

    # Validate ISO timestamp format
    try:
        raw_ts = str(data["event_timestamp"]).replace("Z", "+00:00")
        datetime.fromisoformat(raw_ts)
    except Exception as e:
        raise ValueError(
            f"Line {line_num}: Invalid ISO event_timestamp '{data['event_timestamp']}': {e}"
        )


def publish_messages():
    if not PROJECT_ID or not TOPIC_ID:
        raise ValueError(
            "Missing 'GCP_PROJECT_ID' (or 'PROJECT_ID') or 'PUBSUB_TOPIC' in .env file."
        )

    if not os.path.isfile(FILE_PATH):
        raise FileNotFoundError(f"JSONL file not found at path: {FILE_PATH}")

    publisher = pubsub_v1.PublisherClient()

    if TOPIC_ID.startswith("projects/"):
        topic_path = TOPIC_ID
    else:
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing to topic: {topic_path}\n")

    published_count = 0
    futures = []

    with open(FILE_PATH, "r", encoding="utf-8") as file:
        for line_num, line in enumerate(file, 1):
            line = line.strip()
            if not line:
                continue

            # 1. Parse and validate record schema
            try:
                data = json.loads(line)
                validate_record(data, line_num)
            except (json.JSONDecodeError, ValueError) as err:
                print(f"[SKIP] Invalid payload on line {line_num}: {err}")
                continue

            # 2. Publish message
            data_bytes = json.dumps(data).encode("utf-8")
            future = publisher.publish(topic_path, data=data_bytes)
            futures.append(future)

            published_count += 1
            print(f"[{published_count}] Queued message for order_id: {data['order_id']}")

    # 3. Wait for all async publishes to complete
    for future in futures:
        future.result()

    print(f"\nDone. Successfully published {published_count} valid messages to {topic_path}.")


if __name__ == "__main__":
    publish_messages()