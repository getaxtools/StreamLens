# StreamLens dev Kafka environment

A disposable, local-only Kafka cluster for trying out StreamLens against real
broker behavior. Useful for evaluating the app, reproducing a bug, or testing
a release without pointing it at a cluster you care about.

## What it starts

| Service | Purpose | Port |
|---|---|---|
| `kafka` | Single-node Apache Kafka broker (KRaft mode, no ZooKeeper) | `localhost:9092` |
| `schema-registry` | Confluent Schema Registry, for testing schema-aware decoding | `localhost:8081` |
| `seed` | One-shot container that creates topics and produces sample messages, then exits | n/a |

## Usage

```bash
cd docker
docker compose up -d
```

Wait for `seed` to finish (`docker compose logs -f seed`) - it exits `0` once
topics are created and sample data is produced. Then point StreamLens at:

- **Bootstrap servers:** `localhost:9092`
- **Security protocol:** Plaintext
- **Schema Registry URL (optional):** `http://localhost:8081`
- **Environment tag:** `Local` (or `Development`)

Tear down (keeps the volume, so a restart replays the same seeded topics only
if `seed` is re-run manually - see below):

```bash
docker compose down
```

Full reset, including Kafka's on-disk log data:

```bash
docker compose down -v
```

## Seeded topics

| Topic | Partitions | Messages | Contents |
|---|---|---|---|
| `order-created` | 3 | 200 | JSON order events with `orderId`, `customerId`, line items, total |
| `payment-processed` | 3 | 200 | JSON payment events keyed by the same `orderId` |
| `order-shipped` | 3 | ~133 | JSON shipment events (only produced for ~2/3 of orders, intentionally) |
| `orders.dlq` | 1 | 200 | Poison messages with an `error` field, simulating failed validation |
| `user-events` | 1 | 200 | Clickstream-style events (`login`, `page_view`, `checkout_completed`, etc.) |
| `inventory-updates` | 2 | 200 | Per-SKU stock deltas (`restock` / `sale`) across 4 warehouses |
| `customer-support-tickets` | 1 | 200 | Support tickets with priority, status, and a nested message thread |
| `product-catalog` | 1 | 200 | Product records with attributes (color, warranty) |
| `audit-log` | 1 | 200 | Synthetic audit entries mirroring the app's audit action types |
| `telemetry-events` | 3 | 5000 | High-volume device-metric readings - for testing virtualized grid scrolling and performance under load |

Every message's JSON payload is padded with a `_padding` field so the
serialized size is **at least 10KB**, even where the real fields are small.
That way large-message rendering can be tested across the whole topic set, not
just `telemetry-events`.

Every message also carries `trace-id`, `content-type`, and `schema-version`
headers, so header display and filtering can be exercised against realistic
data. Messages sharing an `orderId` key across `order-created` /
`payment-processed` / `order-shipped` are a ready-made example for testing the
correlated multi-topic view.

## Re-seeding

The `seed` container only runs once per `docker compose up`. To produce a
fresh batch of sample messages without restarting the whole stack:

```bash
docker compose up seed --build --force-recreate
```

## Notes

- Local development and manual/demo testing only. There's no auth, no
  persistence guarantee beyond the named Docker volume, and nothing here is
  suitable for production data.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is off so the topic list in StreamLens
  matches exactly what `seed_data.py` created, which helps when testing the
  topic tree against a known set.
