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

For a step-by-step walkthrough of the StreamLens connection dialog itself - which
fields to fill for each connection type - see
[`docs/testing-connections.md`](../docs/testing-connections.md).

Wait for `seed` to finish (`docker compose logs -f seed`) - it exits `0` once
topics are created and sample data is produced. Then point StreamLens at:

- **Bootstrap servers:** `localhost:9092`
- **Security protocol:** Plaintext
- **Schema Registry URL (optional):** `http://localhost:8081`
- **Environment tag:** `Local` (or `Development`)

### Extra Avro topics

The default `seed` run covers one happy-path Avro topic. A second script adds
schema evolution, logical types, complex types, and three topics of
deliberately undecodable messages:

```bash
docker compose run --rm seed python seed_avro_edge_cases.py
```

Safe to re-run. See [testing Avro](../docs/testing-avro.md) for what each topic
is for and what to check.

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

Three more go through Schema Registry, so they need the **Schema Registry URL**
field set (`http://localhost:8081`) and the topic's format set to Avro or
Protobuf. Viewed as String they look like binary, which is correct:

| Topic | Partitions | Messages | Format | Contents |
|---|---|---|---|---|
| `sensor-readings-avro` | 2 | 200 | Avro | Flat record with a nullable field and an array - everyday Avro decoding |
| `device-telemetry-avro` | 3 | 200 | Avro | Logical types (`timestamp-millis`), an enum, `fixed`, `bytes`, two nullable numeric unions, a map, a nested record, and an array of doubles |
| `shipment-events-proto` | 2 | 200 | Protobuf | Nested messages, an enum, a map, and a repeated field, produced through Confluent's `ProtobufSerializer` |

`device-telemetry-avro` carries the shapes where the decoded JSON drops
information only the schema still has: a map and an empty record both render as
`{}`, and nullable `int` and `float` are both bare JSON numbers. It checks that
decoding handles them, and it's the fixture for re-encoding Avro once publishing
supports it.

`shipment-events-proto` deliberately declares `GeoPoint` before `ShipmentEvent`
in [`seed/shipment_events.proto`](seed/shipment_events.proto), which makes
Confluent write a message-index of `[1]` into every payload rather than the
single-byte shortcut. Do not reorder those messages - see the comment in the
file for why.

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

## TLS / mTLS stack (optional)

`docker-compose.tls.yml` starts a **second, independent** cluster for exercising the
TLS, mutual-TLS, and Schema Registry authentication paths. It does not touch the
plaintext stack above - different containers, different ports, its own volumes -
so both can run at the same time.

```bash
cd docker
docker compose -f docker-compose.tls.yml up -d
```

First start takes a couple of minutes: a one-shot `certs` container generates a
throwaway CA and certificates, then the broker, Schema Registry, and seeder come
up in order.

| Service | Purpose | Port |
|---|---|---|
| `kafka-tls` | Broker with an SSL and a SASL_SSL listener | `localhost:9093` (SSL), `localhost:9094` (SASL_SSL) |
| `schema-registry-tls` | Schema Registry over HTTPS with basic auth | `localhost:8082` |
| `certs` | One-shot; generates the CA and certificates, then exits | n/a |
| `scram-init` | One-shot; creates the SCRAM user, then exits | n/a |
| `seed-tls` | One-shot; seeds the same topics as the plaintext stack | n/a |

### Getting the certificates onto your machine

The certificates live in a Docker volume. Copy them somewhere StreamLens can read:

```bash
mkdir -p "$HOME/streamlens-certs"
docker run --rm -v streamlens-tls_certs:/certs -v "$HOME/streamlens-certs:/out"   alpine sh -c "cp /certs/ca.pem /certs/client.pem /certs/client.key /out/"
```

### Connecting

**Mutual TLS** (port 9093 - the broker requires a client certificate):

- **Bootstrap servers:** `localhost:9093`
- **Security protocol:** `Ssl`
- **CA Certificate Path:** `<your path>/ca.pem`
- **Client Certificate Path:** `<your path>/client.pem`
- **Client Key Path:** `<your path>/client.key`

**SASL_SSL with SCRAM** (port 9094 - encrypted transport, password login):

- **Bootstrap servers:** `localhost:9094`
- **Security protocol:** `SaslSsl`
- **SASL Mechanism:** `ScramSha512`
- **SASL Username / Password:** `streamlens` / `streamlens-secret`
- **CA Certificate Path:** `<your path>/ca.pem`

**Schema Registry** (either of the above):

- **Schema Registry URL:** `https://localhost:8082`
- **Username / Password:** `registry` / `registry-secret`

The CA path is required in every case: these certificates are signed by a private
CA that nothing in the Windows trust store knows about. That is the point - it is
the case a public-CA cluster never exercises. If you would rather not point at the
CA file, **Skip certificate verification** works too, though it defeats the purpose.

Tear down (`-v` also discards the generated certificates, so the next start issues
new ones and any saved connection profile needs its paths refreshed):

```bash
docker compose -f docker-compose.tls.yml down -v
```

### Notes on this stack

- Every credential here is a throwaway for local testing. Nothing in `docker/tls/`
  is a secret worth protecting, and none of it should be reused anywhere real.
- The TLS listeners advertise `localhost`, since the client that matters is
  StreamLens running on the host. In-network containers (the seeder) therefore use
  the broker's plaintext internal listener instead.
- The broker sets `ssl.client.auth=required` on port 9093, so a client with no
  certificate is rejected. That is deliberate - it is what makes the mTLS path
  testable rather than merely configurable.

## Kerberos / GSSAPI stack (optional)

`docker-compose.kerberos.yml` starts a throwaway KDC and a broker that accepts
only `SASL_PLAINTEXT/GSSAPI`. Independent of the other two stacks, so all three
can run at once.

```bash
cd docker
docker compose -f docker-compose.kerberos.yml up -d
```

Ports: **9095** for the broker, **8088** for the KDC (udp and tcp), and **8089**
for kadmin. The realm,
the principals, and the passwords all live inside these containers and are
rebuilt on every `up`.

**Read this before testing from Windows.** The underlying Kafka client on
Windows doesn't use MIT Kerberos; it authenticates through native SSPI as your
logged-on Windows user. So keytabs don't work there at all, and this stack's
standalone realm has no relationship to your Windows login, which means SSPI
has no ticket to present. Use this stack to verify the broker side and a
Linux or WSL client. Verifying the Windows path needs a domain-joined machine
and an AD-registered service principal.

**Kerberos binds a service ticket to the hostname the client connected to**,
so a host-side client must reach the broker as `kafka-krb` rather than
`localhost`. Add `127.0.0.1 kafka-krb` to your hosts file.

The stack writes `client.keytab` and `krb5.host.conf` into
`docker/kerberos/out/` for a host-side client. Then connect with:

| Field | Value |
|---|---|
| Bootstrap servers | `kafka-krb:9095` |
| Security protocol | `SASL_PLAINTEXT` |
| SASL mechanism | `GSSAPI` |
| Kerberos service name | `kafka` |
| Kerberos principal | `streamlens@STREAMLENS.TEST` (optional; the ambient ticket is used if blank) |
| Kerberos keytab | `docker/kerberos/out/client.keytab` (ignored on Windows) |

Tear down with `-v`, which matters here: the keytabs volume holds keys for a
realm that gets recreated from scratch next time, and a stale keytab
authenticates against nothing.

```bash
docker compose -f docker-compose.kerberos.yml down -v
```

## Notes

- Local development and manual/demo testing only. There's no auth, no
  persistence guarantee beyond the named Docker volume, and nothing here is
  suitable for production data.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is off so the topic list in StreamLens
  matches exactly what `seed_data.py` created, which helps when testing the
  topic tree against a known set.
