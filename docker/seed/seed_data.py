"""
Seeds the dev Kafka cluster with topics and sample messages so the app has
something realistic to browse on first connect.

The topics follow the running example from docs/streamlens-studio-spec.md
(order-created / payment-processed / order-shipped / orders.dlq), which gives
the correlated multi-topic view, header search and JSON pretty-print something
to show. The rest add variety - inventory, support tickets, catalog, audit log
- and telemetry-events is the high-volume/large-payload one for grid
virtualization and large-message rendering (spec Section 3.2 / Section 5).

Payloads are padded with a "_padding" field up to MIN_MESSAGE_BYTES so even
the naturally small topics exercise large-message handling.
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

MIN_MESSAGE_BYTES = 10 * 1024
MESSAGES_PER_TOPIC = 200
HIGH_VOLUME_TOPIC = "telemetry-events"
HIGH_VOLUME_MESSAGE_COUNT = 5000

# Spec Section 3.3: an Avro topic in the Confluent wire format (0x00 magic byte, big-endian
# schema id, Avro body) so the registry decoding path has real data. Everything else seeded
# here is JSON and needs no registry.
AVRO_TOPIC = "sensor-readings-avro"

AVRO_SCHEMA = """
{
  "type": "record",
  "name": "SensorReading",
  "namespace": "com.streamlens.demo",
  "fields": [
    { "name": "sensorId", "type": "string" },
    { "name": "site", "type": "string" },
    { "name": "celsius", "type": "double" },
    { "name": "humidity", "type": "double" },
    { "name": "readingCount", "type": "int" },
    { "name": "healthy", "type": "boolean" },
    { "name": "firmware", "type": ["null", "string"], "default": null },
    { "name": "tags", "type": { "type": "array", "items": "string" } }
  ]
}
"""

TOPIC_SPECS = [
    ("order-created", 3),
    ("payment-processed", 3),
    ("order-shipped", 3),
    ("orders.dlq", 1),
    ("user-events", 1),
    ("inventory-updates", 2),
    ("customer-support-tickets", 1),
    ("product-catalog", 1),
    ("audit-log", 1),
    (HIGH_VOLUME_TOPIC, 3),
    (AVRO_TOPIC, 2),
]

CUSTOMERS = [
    {"id": "cus_1001", "name": "Asha Rao", "email": "asha.rao@example.com"},
    {"id": "cus_1002", "name": "Ben Ortiz", "email": "ben.ortiz@example.com"},
    {"id": "cus_1003", "name": "Chidi Okeke", "email": "chidi.okeke@example.com"},
    {"id": "cus_1004", "name": "Dana Kim", "email": "dana.kim@example.com"},
    {"id": "cus_1005", "name": "Elena Petrova", "email": "elena.petrova@example.com"},
]

SKUS = [
    {"sku": "SKU-A1", "name": "Wireless Mouse", "price": 19.99, "category": "Accessories"},
    {"sku": "SKU-B7", "name": "Mechanical Keyboard", "price": 89.5, "category": "Accessories"},
    {"sku": "SKU-C3", "name": "USB-C Hub", "price": 34.0, "category": "Accessories"},
    {"sku": "SKU-D9", "name": "27-inch Monitor", "price": 249.0, "category": "Displays"},
    {"sku": "SKU-E2", "name": "Laptop Stand", "price": 45.5, "category": "Accessories"},
]

REGIONS = ["us-east-1", "us-west-2", "eu-central-1", "ap-south-1"]
DEVICE_TYPES = ["desktop", "mobile", "tablet", "server-agent"]


def pad_to_min_size(payload: dict, min_bytes: int = MIN_MESSAGE_BYTES) -> dict:
    """Pads the payload out to min_bytes with a readable filler field, leaving the
    real fields alone."""
    baseline = len(json.dumps(payload).encode("utf-8"))
    shortfall = min_bytes - baseline
    if shortfall <= 0:
        return payload

    # Repeating text rather than random bytes, so it reads as padding in the detail
    # pane instead of looking like corrupt data.
    unit = "lorem-ipsum-streamlens-filler-0123456789-"
    repeats = (shortfall // len(unit)) + 1
    payload["_padding"] = (unit * repeats)[:shortfall]
    return payload


def connect_admin(retries: int = 20, delay: float = 3.0) -> AdminClient:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for attempt in range(1, retries + 1):
        try:
            cluster_metadata = admin.list_topics(timeout=5)
            if cluster_metadata.brokers:
                return admin
        except Exception as ex:  # broker not reachable/ready yet
            print(f"[seed] broker not ready yet (attempt {attempt}/{retries}): {ex}")
        time.sleep(delay)
    print("[seed] ERROR: could not reach Kafka broker, giving up.")
    sys.exit(1)


def create_topics(admin: AdminClient) -> None:
    new_topics = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name, partitions in TOPIC_SPECS
    ]
    futures = admin.create_topics(new_topics, request_timeout=15)

    for name, future in futures.items():
        try:
            future.result()
            print(f"[seed] created topic: {name}")
        except Exception as ex:
            if "already exists" in str(ex).lower():
                print(f"[seed] topic already exists, skipping: {name}")
            else:
                print(f"[seed] ERROR creating topic {name}: {ex}")
                sys.exit(1)


def header(trace_id: str, content_type: str = "application/json", schema_version: str = "1"):
    return [
        ("trace-id", trace_id.encode("utf-8")),
        ("content-type", content_type.encode("utf-8")),
        ("schema-version", schema_version.encode("utf-8")),
    ]


def now_iso(offset_seconds: float = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[seed] ERROR delivering message to {msg.topic()}: {err}")


def produce_json(producer: Producer, topic: str, key: str, payload: dict, trace_id: str, schema_version: str = "1") -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(pad_to_min_size(payload)).encode("utf-8"),
        headers=header(trace_id, schema_version=schema_version),
        callback=delivery_report,
    )


def seed_order_lifecycle(producer: Producer, order_index: int) -> None:
    order_id = f"ord_{2000 + order_index}"
    trace_id = str(uuid.uuid4())
    customer = CUSTOMERS[order_index % len(CUSTOMERS)]
    item = SKUS[order_index % len(SKUS)]
    quantity = (order_index % 3) + 1
    total = round(item["price"] * quantity, 2)

    order_created = {
        "orderId": order_id,
        "customerId": customer["id"],
        "customerName": customer["name"],
        "items": [{"sku": item["sku"], "name": item["name"], "price": item["price"], "quantity": quantity}],
        "total": total,
        "createdAt": now_iso(),
    }
    produce_json(producer, "order-created", order_id, order_created, trace_id)

    payment_processed = {
        "orderId": order_id,
        "paymentId": f"pay_{3000 + order_index}",
        "amount": total,
        "currency": "USD",
        "status": "SUCCEEDED",
        "processedAt": now_iso(2),
    }
    produce_json(producer, "payment-processed", order_id, payment_processed, trace_id)

    # Every third order ships and the rest stay pending, so the topics aren't a
    # perfect 1:1 match.
    if order_index % 3 != 2:
        order_shipped = {
            "orderId": order_id,
            "carrier": "UPS" if order_index % 2 == 0 else "FedEx",
            "trackingNumber": f"1Z{order_index:06d}TRK",
            "shippedAt": now_iso(5),
        }
        produce_json(producer, "order-shipped", order_id, order_shipped, trace_id)

    producer.poll(0)


def seed_dlq(producer: Producer, count: int) -> None:
    error_reasons = [
        "Missing required field: customerId",
        "Schema validation failed: amount must be positive",
        "Unknown SKU referenced in items[]",
        "Malformed timestamp in createdAt field",
        "Duplicate orderId detected during dedup check",
    ]
    for i in range(count):
        order_id = f"ord_9{i:03d}"
        trace_id = str(uuid.uuid4())
        msg = {
            "originalTopic": ["order-created", "payment-processed"][i % 2],
            "orderId": order_id,
            "error": error_reasons[i % len(error_reasons)],
            "failedAt": now_iso(i),
            "rawValue": json.dumps({"orderId": order_id, "items": []}),
        }
        produce_json(producer, "orders.dlq", order_id, msg, trace_id, schema_version="0")
    producer.poll(0)


def seed_user_events(producer: Producer, count: int) -> None:
    events = ["login", "page_view", "logout", "search", "add_to_cart", "checkout_started", "checkout_completed"]
    for i in range(count):
        customer = CUSTOMERS[i % len(CUSTOMERS)]
        event = {
            "userId": customer["id"],
            "event": events[i % len(events)],
            "device": DEVICE_TYPES[i % len(DEVICE_TYPES)],
            "region": REGIONS[i % len(REGIONS)],
            "timestamp": now_iso(i),
        }
        produce_json(producer, "user-events", customer["id"], event, str(uuid.uuid4()))
    producer.poll(0)


def seed_inventory_updates(producer: Producer, count: int) -> None:
    for i in range(count):
        item = SKUS[i % len(SKUS)]
        delta = (i % 20) - 10
        update = {
            "sku": item["sku"],
            "name": item["name"],
            "category": item["category"],
            "warehouseId": f"wh-{(i % 4) + 1}",
            "quantityDelta": delta,
            "reason": "restock" if delta > 0 else "sale",
            "updatedAt": now_iso(i),
        }
        produce_json(producer, "inventory-updates", item["sku"], update, str(uuid.uuid4()))
    producer.poll(0)


def seed_support_tickets(producer: Producer, count: int) -> None:
    subjects = [
        "Order not delivered",
        "Refund request",
        "Payment charged twice",
        "Item arrived damaged",
        "Question about warranty",
    ]
    priorities = ["low", "medium", "high", "urgent"]
    for i in range(count):
        customer = CUSTOMERS[i % len(CUSTOMERS)]
        ticket_id = f"tix_{5000 + i}"
        ticket = {
            "ticketId": ticket_id,
            "customerId": customer["id"],
            "customerName": customer["name"],
            "subject": subjects[i % len(subjects)],
            "priority": priorities[i % len(priorities)],
            "status": "open" if i % 3 != 0 else "closed",
            "createdAt": now_iso(i),
            "messages": [
                {"author": customer["name"], "text": "Can you help me with this?", "sentAt": now_iso(i)},
            ],
        }
        produce_json(producer, "customer-support-tickets", ticket_id, ticket, str(uuid.uuid4()))
    producer.poll(0)


def seed_product_catalog(producer: Producer, count: int) -> None:
    for i in range(count):
        item = SKUS[i % len(SKUS)]
        catalog_entry = {
            "sku": item["sku"],
            "name": item["name"],
            "category": item["category"],
            "price": item["price"],
            "currency": "USD",
            "description": f"{item['name']} - revision {i}. Durable, reliable, and built for daily use.",
            "attributes": {
                "color": ["black", "silver", "white"][i % 3],
                "warrantyMonths": 12 + (i % 3) * 12,
            },
            "publishedAt": now_iso(i),
        }
        produce_json(producer, "product-catalog", item["sku"], catalog_entry, str(uuid.uuid4()))
    producer.poll(0)


def seed_audit_log(producer: Producer, count: int) -> None:
    action_types = ["ClusterProfile.Created", "Message.Produced", "Topic.Deleted", "ConsumerGroup.OffsetReset"]
    for i in range(count):
        event_id = str(uuid.uuid4())
        entry = {
            "id": event_id,
            "occurredAt": now_iso(i),
            "actionType": action_types[i % len(action_types)],
            "resourceName": f"resource-{i}",
            "environment": ["Local", "Development", "Staging"][i % 3],
            "details": f"Seed-generated audit entry #{i} for local testing.",
        }
        produce_json(producer, "audit-log", event_id, entry, str(uuid.uuid4()))
    producer.poll(0)


def seed_telemetry_events(producer: Producer, count: int) -> None:
    """High-volume, large-payload topic for grid scrolling and large-message
    rendering (spec Section 3.2 / Section 5)."""
    metrics = ["cpu_percent", "memory_percent", "disk_io_ops", "network_latency_ms", "queue_depth"]
    for i in range(count):
        device_id = f"device-{i % 50:03d}"
        reading = {
            "deviceId": device_id,
            "region": REGIONS[i % len(REGIONS)],
            "deviceType": DEVICE_TYPES[i % len(DEVICE_TYPES)],
            "sequence": i,
            "metrics": {metric: round(((i * (idx + 1)) % 100) + (idx * 0.37), 3) for idx, metric in enumerate(metrics)},
            "samples": [round((i + s) % 100 + s * 0.1, 2) for s in range(20)],
            "recordedAt": now_iso(i * 0.05),
        }
        produce_json(producer, HIGH_VOLUME_TOPIC, device_id, reading, str(uuid.uuid4()))
        if i % 500 == 0:
            producer.poll(0)
    producer.poll(0)


def seed_avro_sensor_readings(count: int) -> None:
    """Produces Avro-encoded messages through Schema Registry.

    Has its own Producer rather than sharing the JSON one - AvroSerializer returns bytes
    already serialized for a specific topic/field context, and mixing the two makes it
    unclear which topics are registry-backed.
    """
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(registry, AVRO_SCHEMA)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    sites = ["lisbon-dc1", "dublin-dc2", "singapore-dc3"]
    firmwares = [None, "v2.1.4", "v2.2.0"]

    for i in range(count):
        sensor_id = f"sensor_{i % 25:04d}"
        reading = {
            "sensorId": sensor_id,
            "site": sites[i % len(sites)],
            "celsius": round(18.0 + (i % 170) / 10.0, 2),
            "humidity": round(35.0 + (i % 400) / 10.0, 2),
            "readingCount": i,
            "healthy": (i % 11) != 0,
            "firmware": firmwares[i % len(firmwares)],
            "tags": ["rack-a", "prod"] if i % 2 == 0 else ["rack-b"],
        }
        producer.produce(
            topic=AVRO_TOPIC,
            key=sensor_id.encode("utf-8"),
            value=serializer(reading, SerializationContext(AVRO_TOPIC, MessageField.VALUE)),
        )

    remaining = producer.flush(60)
    if remaining > 0:
        print(f"[seed] WARNING: {remaining} Avro messages undelivered")
        sys.exit(1)
    print(f"[seed] seeded {AVRO_TOPIC} ({count} Avro msgs via Schema Registry)")


def main() -> None:
    admin = connect_admin()
    create_topics(admin)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "streamlens-seed-producer",
        "queue.buffering.max.messages": 200000,
    })

    # order-created/payment-processed/order-shipped share an orderId, one triplet per
    # iteration. So MESSAGES_PER_TOPIC orders gives that many messages in the first two
    # and about 2/3 as many in order-shipped (see seed_order_lifecycle).
    for order_index in range(MESSAGES_PER_TOPIC):
        seed_order_lifecycle(producer, order_index)

    seed_dlq(producer, MESSAGES_PER_TOPIC)
    seed_user_events(producer, MESSAGES_PER_TOPIC)
    seed_inventory_updates(producer, MESSAGES_PER_TOPIC)
    seed_support_tickets(producer, MESSAGES_PER_TOPIC)
    seed_product_catalog(producer, MESSAGES_PER_TOPIC)
    seed_audit_log(producer, MESSAGES_PER_TOPIC)
    seed_telemetry_events(producer, HIGH_VOLUME_MESSAGE_COUNT)

    remaining = producer.flush(120)
    if remaining > 0:
        print(f"[seed] WARNING: {remaining} messages were not delivered before flush timeout")
        sys.exit(1)

    # After the JSON flush. Avro has its own producer and serializer (see that function's
    # docstring). A registry that can't be reached shouldn't undo the JSON topics already
    # seeded, so report it and carry on instead of failing the run.
    try:
        seed_avro_sensor_readings(MESSAGES_PER_TOPIC)
    except Exception as exc:  # noqa: BLE001 - seeding is best-effort for the Avro slice
        print(f"[seed] WARNING: Avro seeding skipped ({exc})")

    print(
        "[seed] done: seeded order-created, payment-processed, order-shipped, orders.dlq, "
        "user-events, inventory-updates, customer-support-tickets, product-catalog, "
        f"audit-log ({MESSAGES_PER_TOPIC} msgs each), {HIGH_VOLUME_TOPIC} "
        f"({HIGH_VOLUME_MESSAGE_COUNT} msgs), and {AVRO_TOPIC} (Avro via Schema Registry); "
        f"every JSON message >= {MIN_MESSAGE_BYTES} bytes"
    )


if __name__ == "__main__":
    main()
