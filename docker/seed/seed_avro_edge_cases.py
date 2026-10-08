"""
Seeds Avro topics that exercise the decode paths the main seeder doesn't.

seed_data.py produces one well-formed schema on the happy path, which proves
decoding works but not what happens when it can't. This adds the cases that
actually break Avro viewers:

  - schema evolution: three registered versions on one subject, messages from
    every version live in the same topic (a reader pinned to "latest" gets
    these wrong)
  - logical types: decimal/date/timestamp/uuid, which are annotated primitives
    and render as raw longs or byte arrays when the annotation is ignored
  - nested records, maps, enums, unions, fixed, and a parent reference
  - unframed and corrupt payloads: plain UTF-8 JSON on an Avro topic, a bad
    magic byte, and a valid frame pointing at an unregistered schema id

The corrupt topics are the point of this script. A viewer should show a clear
per-message decode error and keep rendering the rest of the topic; a crash,
a blank pane, or a silent fallback to raw bytes is a bug.

Run after seed_data.py. Safe to re-run - topics are created if absent and
messages are appended.
"""

import json
import os
import struct
import sys
import uuid
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# Reuse the main seeder's connection handling so both scripts behave identically
# against the plaintext and TLS stacks.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data import connect_admin, kafka_config, schema_registry_config  # noqa: E402

MESSAGES_PER_VERSION = 40

EVOLUTION_TOPIC = "avro-evolution"
LOGICAL_TOPIC = "avro-logical-types"
COMPLEX_TOPIC = "avro-complex"
UNFRAMED_TOPIC = "avro-unframed"
BAD_MAGIC_TOPIC = "avro-bad-magic"
UNKNOWN_ID_TOPIC = "avro-unknown-schema-id"

TOPIC_SPECS = [
    (EVOLUTION_TOPIC, 2),
    (LOGICAL_TOPIC, 1),
    (COMPLEX_TOPIC, 2),
    (UNFRAMED_TOPIC, 1),
    (BAD_MAGIC_TOPIC, 1),
    (UNKNOWN_ID_TOPIC, 1),
]

# --- Schema evolution -------------------------------------------------------
# Three compatible versions on one subject. Each only adds optional fields with
# defaults, so a v1 reader can still read v3 data and vice versa. All three
# versions' messages stay in the topic.

EVOLUTION_V1 = """
{
  "type": "record",
  "name": "CustomerProfile",
  "namespace": "com.streamlens.demo",
  "fields": [
    { "name": "customerId", "type": "string" },
    { "name": "email", "type": "string" }
  ]
}
"""

EVOLUTION_V2 = """
{
  "type": "record",
  "name": "CustomerProfile",
  "namespace": "com.streamlens.demo",
  "fields": [
    { "name": "customerId", "type": "string" },
    { "name": "email", "type": "string" },
    { "name": "displayName", "type": ["null", "string"], "default": null },
    { "name": "loyaltyTier", "type": ["null", "string"], "default": null }
  ]
}
"""

# v3 adds a long and a nested optional record - the evolutions most likely to be
# mis-decoded by a reader pinned to an older version.
EVOLUTION_V3 = """
{
  "type": "record",
  "name": "CustomerProfile",
  "namespace": "com.streamlens.demo",
  "fields": [
    { "name": "customerId", "type": "string" },
    { "name": "email", "type": "string" },
    { "name": "displayName", "type": ["null", "string"], "default": null },
    { "name": "loyaltyTier", "type": ["null", "string"], "default": null },
    { "name": "lifetimeValue", "type": ["null", "long"], "default": null },
    {
      "name": "address",
      "type": ["null", {
        "type": "record",
        "name": "Address",
        "fields": [
          { "name": "line1", "type": "string" },
          { "name": "city", "type": "string" },
          { "name": "country", "type": "string" }
        ]
      }],
      "default": null
    }
  ]
}
"""

# --- Logical types ----------------------------------------------------------
# Annotated primitives. Ignore the annotation and `amount` renders as raw bytes,
# the dates as bare ints, and the timestamps as 13-digit longs.

LOGICAL_SCHEMA = """
{
  "type": "record",
  "name": "LedgerEntry",
  "namespace": "com.streamlens.demo",
  "fields": [
    { "name": "entryId", "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "amount", "type": { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2 } },
    { "name": "bookedOn", "type": { "type": "int", "logicalType": "date" } },
    { "name": "bookedAt", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "settledAt", "type": ["null", { "type": "long", "logicalType": "timestamp-micros" }], "default": null },
    { "name": "processingTime", "type": { "type": "int", "logicalType": "time-millis" } }
  ]
}
"""

# --- Complex types ----------------------------------------------------------
# Nested records, an enum, maps, an array of records, a union of two record
# types, and fixed - the shapes that break naive tree renderers.

COMPLEX_SCHEMA = """
{
  "type": "record",
  "name": "WorkflowRun",
  "namespace": "com.streamlens.demo",
  "fields": [
    { "name": "runId", "type": "string" },
    {
      "name": "status",
      "type": { "type": "enum", "name": "RunStatus", "symbols": ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"] }
    },
    { "name": "labels", "type": { "type": "map", "values": "string" } },
    { "name": "retryCounts", "type": { "type": "map", "values": "int" } },
    {
      "name": "steps",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Step",
          "fields": [
            { "name": "name", "type": "string" },
            { "name": "durationMs", "type": "long" },
            { "name": "output", "type": ["null", "string", "bytes"], "default": null }
          ]
        }
      }
    },
    {
      "name": "trigger",
      "type": [
        { "type": "record", "name": "ScheduleTrigger", "fields": [{ "name": "cron", "type": "string" }] },
        { "type": "record", "name": "ManualTrigger", "fields": [{ "name": "actor", "type": "string" }] }
      ]
    },
    { "name": "checksum", "type": { "type": "fixed", "name": "Md5", "size": 16 } },
    {
      "name": "parent",
      "type": ["null", { "type": "record", "name": "ParentRef", "fields": [
        { "name": "runId", "type": "string" },
        { "name": "depth", "type": "int" }
      ]}],
      "default": null
    }
  ]
}
"""

TIERS = ["bronze", "silver", "gold", "platinum"]
CITIES = [("Lisbon", "PT"), ("Dublin", "IE"), ("Singapore", "SG"), ("Austin", "US")]
STATUSES = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"]


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[avro-seed] ERROR delivering to {msg.topic()}: {err}")


def create_topics(admin: AdminClient) -> None:
    new_topics = [
        NewTopic(name, num_partitions=parts, replication_factor=1)
        for name, parts in TOPIC_SPECS
    ]
    for name, future in admin.create_topics(new_topics, request_timeout=15).items():
        try:
            future.result()
            print(f"[avro-seed] created topic: {name}")
        except Exception as ex:
            if "already exists" in str(ex).lower():
                print(f"[avro-seed] topic exists, skipping: {name}")
            else:
                print(f"[avro-seed] ERROR creating topic {name}: {ex}")
                sys.exit(1)


def seed_evolution(registry: SchemaRegistryClient, producer: Producer) -> None:
    """Registers v1/v2/v3 in order on one subject, producing with each as it goes.

    Registering in sequence is what creates real version history - the subject ends
    up with three versions and the topic holds messages framed with all three ids.
    """
    subject = f"{EVOLUTION_TOPIC}-value"

    for version, schema_str in enumerate([EVOLUTION_V1, EVOLUTION_V2, EVOLUTION_V3], start=1):
        serializer = AvroSerializer(registry, schema_str)
        ctx = SerializationContext(EVOLUTION_TOPIC, MessageField.VALUE)

        for i in range(MESSAGES_PER_VERSION):
            idx = (version - 1) * MESSAGES_PER_VERSION + i
            customer_id = f"cus_{7000 + idx}"
            record = {"customerId": customer_id, "email": f"user{idx}@example.com"}

            if version >= 2:
                record["displayName"] = None if i % 5 == 0 else f"User {idx}"
                record["loyaltyTier"] = TIERS[i % len(TIERS)]

            if version >= 3:
                record["lifetimeValue"] = None if i % 7 == 0 else (idx * 1337)
                city, country = CITIES[i % len(CITIES)]
                record["address"] = None if i % 4 == 0 else {
                    "line1": f"{100 + idx} Example Street",
                    "city": city,
                    "country": country,
                }

            producer.produce(
                topic=EVOLUTION_TOPIC,
                key=customer_id.encode("utf-8"),
                value=serializer(record, ctx),
                headers=[("schema-version", str(version).encode("utf-8"))],
                callback=delivery_report,
            )
        producer.flush(30)
        print(f"[avro-seed] {EVOLUTION_TOPIC}: v{version} registered, {MESSAGES_PER_VERSION} msgs")

    try:
        versions = registry.get_versions(subject)
        print(f"[avro-seed] subject {subject} now has versions {versions}")
    except Exception as ex:  # noqa: BLE001 - informational only
        print(f"[avro-seed] could not list versions for {subject}: {ex}")


def seed_logical_types(registry: SchemaRegistryClient, producer: Producer) -> None:
    serializer = AvroSerializer(registry, LOGICAL_SCHEMA)
    ctx = SerializationContext(LOGICAL_TOPIC, MessageField.VALUE)
    now = datetime.now(timezone.utc)

    for i in range(MESSAGES_PER_VERSION * 2):
        booked = now - timedelta(days=i, minutes=i * 7)
        entry_id = str(uuid.uuid4())
        record = {
            "entryId": entry_id,
            # Decimal with scale 2 - the serializer handles the bytes conversion.
            "amount": Decimal(f"{(i * 197) % 100000}.{i % 100:02d}"),
            "bookedOn": booked.date(),
            "bookedAt": booked,
            "settledAt": None if i % 6 == 0 else booked + timedelta(hours=3),
            # time-millis takes a datetime.time, not a duration.
            "processingTime": time(hour=(i * 3) % 24, minute=(i * 7) % 60, second=i % 60, microsecond=(i % 1000) * 1000),
        }
        producer.produce(
            topic=LOGICAL_TOPIC,
            key=entry_id.encode("utf-8"),
            value=serializer(record, ctx),
            callback=delivery_report,
        )
    producer.flush(30)
    print(f"[avro-seed] {LOGICAL_TOPIC}: {MESSAGES_PER_VERSION * 2} msgs (decimal/date/timestamp/uuid/time)")


def seed_complex(registry: SchemaRegistryClient, producer: Producer) -> None:
    serializer = AvroSerializer(registry, COMPLEX_SCHEMA)
    ctx = SerializationContext(COMPLEX_TOPIC, MessageField.VALUE)

    for i in range(MESSAGES_PER_VERSION * 2):
        run_id = f"run_{9000 + i}"
        step_count = (i % 5) + 1
        record = {
            "runId": run_id,
            "status": STATUSES[i % len(STATUSES)],
            "labels": {"env": ["dev", "staging", "prod"][i % 3], "team": f"team-{i % 4}", "owner": f"user{i}"},
            "retryCounts": {f"step-{s}": (i + s) % 4 for s in range(step_count)},
            "steps": [
                {
                    "name": f"step-{s}",
                    "durationMs": (i + 1) * (s + 1) * 137,
                    # Cycles through all three union branches so each renders.
                    "output": None if s % 3 == 0 else (f"ok: step {s}" if s % 3 == 1 else b"\x00\x01\x02binary"),
                }
                for s in range(step_count)
            ],
            "trigger": {"cron": "0 */4 * * *"} if i % 2 == 0 else {"actor": f"user{i}@example.com"},
            "checksum": bytes((i + b) % 256 for b in range(16)),
            "parent": None if i % 3 == 0 else {"runId": f"run_{8000 + i}", "depth": i % 4},
        }
        producer.produce(
            topic=COMPLEX_TOPIC,
            key=run_id.encode("utf-8"),
            value=serializer(record, ctx),
            callback=delivery_report,
        )
    producer.flush(30)
    print(f"[avro-seed] {COMPLEX_TOPIC}: {MESSAGES_PER_VERSION * 2} msgs (enum/map/array/union/fixed/nested)")


def seed_malformed(producer: Producer, valid_frame: bytes) -> None:
    """The three ways an 'Avro' message isn't one.

    Each topic isolates a single failure so a decode error can be attributed
    precisely. A good viewer reports these per message and keeps going.
    """
    # 1. Plain UTF-8 JSON on a topic you'd view as Avro - no magic byte, no
    #    schema id. This is exactly what a producer writes when it treats an
    #    Avro topic as String.
    for i in range(20):
        payload = json.dumps({"sensorId": f"sensor_{i:04d}", "celsius": 21.5 + i, "note": "unframed UTF-8, not Avro"})
        producer.produce(
            topic=UNFRAMED_TOPIC,
            key=f"sensor_{i:04d}".encode("utf-8"),
            value=payload.encode("utf-8"),
            callback=delivery_report,
        )

    # 2. Correct structure, wrong magic byte (0x01 instead of 0x00). Catches a
    #    decoder that skips the first 5 bytes without validating them.
    for i in range(20):
        producer.produce(
            topic=BAD_MAGIC_TOPIC,
            key=f"bad_{i:04d}".encode("utf-8"),
            value=b"\x01" + valid_frame[1:],
            callback=delivery_report,
        )

    # 3. Valid magic byte, schema id that was never registered. Forces the
    #    registry lookup to 404 - the decoder should surface that, not hang
    #    or retry forever.
    phantom_id = 999_999
    for i in range(20):
        body = valid_frame[5:]
        producer.produce(
            topic=UNKNOWN_ID_TOPIC,
            key=f"phantom_{i:04d}".encode("utf-8"),
            value=b"\x00" + struct.pack(">I", phantom_id) + body,
            callback=delivery_report,
        )

    producer.flush(30)
    print(f"[avro-seed] {UNFRAMED_TOPIC}, {BAD_MAGIC_TOPIC}, {UNKNOWN_ID_TOPIC}: 20 msgs each (decode-failure cases)")


def main() -> None:
    admin = connect_admin()
    create_topics(admin)

    registry = SchemaRegistryClient(schema_registry_config())
    producer = Producer(kafka_config(**{"client.id": "streamlens-avro-edge-seed"}))

    seed_evolution(registry, producer)
    seed_logical_types(registry, producer)
    seed_complex(registry, producer)

    # Build one genuinely valid frame to mutate, so the malformed messages differ
    # from a real one only in the specific way each topic is testing.
    probe = AvroSerializer(registry, EVOLUTION_V1)
    valid_frame = probe(
        {"customerId": "cus_probe", "email": "probe@example.com"},
        SerializationContext(EVOLUTION_TOPIC, MessageField.VALUE),
    )
    seed_malformed(producer, valid_frame)

    remaining = producer.flush(60)
    if remaining > 0:
        print(f"[avro-seed] WARNING: {remaining} messages undelivered")
        sys.exit(1)

    print(
        f"[avro-seed] done: {EVOLUTION_TOPIC} (3 schema versions), {LOGICAL_TOPIC}, "
        f"{COMPLEX_TOPIC}, and 3 decode-failure topics"
    )


if __name__ == "__main__":
    main()
