# Testing schema-aware decoding

How to exercise StreamLens's Schema Registry decoding (Avro and Protobuf)
against the local Docker stack, including the cases that are supposed to fail.
This is the app-side walkthrough. For what the stacks start and which ports
they expose, see [`docker/README.md`](../docker/README.md), and for connection
setup see [testing connections](testing-connections.md).

Most of it is Avro, which has the wider range of edge cases. Protobuf is
check 10.

## Seeding the Avro topics

The default seed run creates two Avro topics and one Protobuf topic. The Avro
edge cases live in a second script that you run on demand:

```bash
cd docker
docker compose up -d                       # if not already running
docker compose run --rm seed python seed_avro_edge_cases.py
```

`docker compose run` waits for the registry's healthcheck before the seeder
starts, so there's no need to wait by hand. If you run a seed script some other
way against a stack you just started, wait until the registry answers first:

```bash
curl -s http://localhost:8081/subjects
```

Seeding before the registry is ready skips every registry-backed topic with a
warning, and the run still exits `0`.

The edge-case script is safe to re-run. Topics are created if absent, messages
are appended, and the three schema versions re-register to the same ids.

## What you get

Seeded by the default run:

| Topic | Messages | What it exercises |
|---|---|---|
| `sensor-readings-avro` | 200 | Happy path: one schema, flat record, optional field, array |
| `device-telemetry-avro` | 200 | `timestamp-millis`, enum, `fixed`, `bytes`, nullable `int` *and* `float`, map, nested record, array of doubles |
| `shipment-events-proto` | 200 | Protobuf: nested messages, enum, map, repeated field (see check 10) |

Seeded by `seed_avro_edge_cases.py`:

| Topic | Messages | Parts | What it exercises |
|---|---|---|---|
| `avro-evolution` | 120 | 2 | **Three schema versions in one topic** (40 each). Subject `avro-evolution-value` has versions 1, 2, 3. |
| `avro-logical-types` | 80 | 1 | `decimal`, `date`, `timestamp-millis`, `timestamp-micros`, `uuid`, `time-millis` |
| `avro-complex` | 80 | 2 | Enum, two maps, array of records, union of two record types, `fixed`, nested optional record, a 3-branch union |
| `avro-unframed` | 20 | 1 | **Must fail.** Plain UTF-8 JSON on an Avro topic, no magic byte |
| `avro-bad-magic` | 20 | 1 | **Must fail.** Valid frame, magic byte `0x01` instead of `0x00` |
| `avro-unknown-schema-id` | 20 | 1 | **Must fail.** Valid magic, schema id `999999`, never registered |

Registry subjects: `sensor-readings-avro-value`, `device-telemetry-avro-value`,
`shipment-events-proto-value`, `avro-evolution-value`,
`avro-logical-types-value`, `avro-complex-value`.

Connect with **Schema Registry URL** `http://localhost:8081` and set the topic
format to **Avro** (or **Protobuf** for `shipment-events-proto`).

> **Schema ids are global and assigned in seeding order**, so they aren't
> stable across rebuilds. Never assume they start at 1. Read them from the
> registry instead:
>
> ```bash
> curl -s http://localhost:8081/subjects/avro-evolution-value/versions/1
> ```

## The checks

### 1. Happy path

Open `sensor-readings-avro` as Avro. Fields decode with names and types.
`firmware` is a nullable union and shows `null` on roughly a third of messages,
and `tags` renders as an array.

### 2. Schema evolution

This is the check most likely to go wrong. Open `avro-evolution`. All 120
messages are in one topic but framed with three different schema ids.

- **Every message decodes.** A reader that resolves the schema per message
  handles all three. One pinned to "latest" mangles or drops the v1 and v2
  messages.
- **v1 messages show two fields** (`customerId`, `email`) and no phantom
  `displayName`/`address` keys.
- **v2 messages add** `displayName` and `loyaltyTier`, with `displayName` null
  on every 5th.
- **v3 messages add** `lifetimeValue` (a long, null on every 7th) and the nested
  `address` record (`line1`, `city`, `country`), expandable and null on every
  4th.
- Each message carries a `schema-version` header (1, 2 or 3). Use it to confirm
  the app's decode matches what was actually written.

What each version should contain:

```
v1: customerId, email
v2: customerId, email, displayName, loyaltyTier
v3: customerId, email, displayName, loyaltyTier, lifetimeValue, address
```

Cross-check the registry directly:

```bash
curl -s http://localhost:8081/subjects/avro-evolution-value/versions
# [1,2,3]
```

There's no in-app schema viewer, so the registry and the `schema-version`
header are the only ground truth for this check.

### 3. Logical types

Open `avro-logical-types`. These are annotated primitives, so ignoring the
annotation produces plausible-looking but wrong output:

- `amount` is a **decimal**, not a byte array. Wrong looks like `b'\x01\xe2@'`
  or a base64 blob.
- `bookedOn` is a **date**, not an int near 20000.
- `bookedAt` and `settledAt` are **timestamps**, not 13- or 16-digit longs.
- `processingTime` is a **time of day**, not an int near 86400000.
- `entryId` is a **uuid** string.

Whichever way the app renders logical types, the detail pane and every export
format should agree.

### 4. Complex types

Open `avro-complex`. Check the detail pane renders all of these without
truncating or flattening them:

- `status` as an enum symbol
- `labels` (map of string) and `retryCounts` (map of int) as maps, not as a
  record with arbitrary keys
- `steps[].output`, a 3-branch union of `null`/`string`/`bytes`, cycling
  through all three
- `trigger`, a union of two *record* types. The branch name should be visible,
  since `{"cron": ...}` and `{"actor": ...}` are different types, not one
  optional record
- `checksum`, a 16-byte `fixed`, shown as hex rather than garbled text
- `parent`, a nested optional record, `null` on every third message

### 5. Decode failures

These three topics contain messages that **cannot** decode as Avro. That is the
test. For each one:

- **The error is per message, not per topic.** Other messages keep rendering.
- **The error says which failure it was.** "Not Confluent-framed", "unexpected
  magic byte 0x01", and "schema id 999999 not found" are three different
  problems, and a user can't act on a generic "decode failed".
- **The app doesn't crash, hang, or blank the pane.**
- **The raw bytes stay reachable.** Switching the topic format to String or
  Hex should show the underlying payload.

| Topic | Expected |
|---|---|
| `avro-unframed` | Starts with `0x7b` (`{`), so it's recognizably not Avro. This is what a producer writes when it sends an Avro topic plain text, and it's the most common Avro mistake. |
| `avro-bad-magic` | Rejected on the magic byte. A decoder that blindly skips 5 bytes would read the valid schema id that follows and produce **garbage that looks like a successful decode**, the worst possible outcome. |
| `avro-unknown-schema-id` | The registry lookup returns 404. It should fail fast with the id named, not retry or hang. |

On `avro-unknown-schema-id`, watch the registry while the 20 messages decode:

```bash
docker compose logs -f schema-registry
```

Note how many lookups the app makes for the same missing id. One per message
means failed lookups aren't cached, which is worth reporting.

### 6. Registry connection failures

With a topic open as Avro, stop the registry and scroll to force new decodes:

```bash
docker compose stop schema-registry
```

The app should report the registry as unreachable and keep the messages it has
already decoded. Restart it and confirm decoding resumes without reconnecting
to the cluster:

```bash
docker compose start schema-registry
```

Also try pointing the connection at `http://localhost:9999` (nothing
listening), and at `http://localhost:8081/wrong` (wrong path, HTTP 404).

### 7. Registry over TLS and basic auth

The TLS stack runs a second registry with HTTPS and basic auth, seeded with the
same topics:

```bash
docker compose -f docker-compose.tls.yml up -d
docker compose -f docker-compose.tls.yml run --rm seed-tls python seed_avro_edge_cases.py
```

Connect with registry URL `https://localhost:8082`, username `registry`,
password `registry-secret`, and the CA path. Confirm that a **wrong password
produces a registry auth error** that's distinct from a broker auth error. The
two services have separate credentials, and mixing them up sends users down the
wrong path.

### 8. Interaction with masking and export

Avro decoding is where masking and export are most likely to regress, because
the value reaching the rule is a decoded object rather than a JSON string.

- Add a field-path rule against an Avro field (e.g. `$.email` on
  `avro-evolution`) and confirm it redacts in the list, the detail pane, **and**
  every export format.
- Export `avro-complex` to CSV and JSON and confirm nested records, maps, and
  `bytes` fields come through intact rather than as language-specific debug
  strings.
- Confirm a message that **failed** to decode exports as something honest
  (raw bytes or an explicit error), not as an empty row.

See [masking and redaction](usageGuide/masking-and-redaction.md).

### 9. Publishing to an Avro or Protobuf topic

StreamLens doesn't encode Avro or Protobuf when it publishes. It sends the
editor text as plain UTF-8, without the Confluent framing (magic byte and
schema id). So any message it publishes to a schema-backed topic is unreadable
to consumers that use the registered schema. The checks below cover the guards
that exist today, and confirm the known gap is still there until it's fixed.

**Guards (builds after v0.6.0):**

- Open a message on `avro-evolution` as **Avro** and choose **Republish**. The
  editor should not open, and the status line should say the format isn't
  supported yet. Repeat on `shipment-events-proto` as **Protobuf**.
- With the topic viewed as Avro, stop the registry and try to publish a
  **New** message. It should be blocked with a "couldn't reach the Schema
  Registry" message rather than published unchecked.
- Publish a **New** message whose JSON violates the registered schema (a
  missing required field, say). It should be rejected with a useful error.

**Known gap.** These checks pass today, but each one writes an unframed record
onto the topic:

- Switch `avro-evolution` to **String** or **JSON**. **Republish** is enabled
  again, because the guard goes by the format you're viewing rather than by the
  topic's registered schema.
- A **New** message whose JSON *matches* the schema is published, as plain
  text, whatever format you're viewing.

Verify what actually landed rather than trusting the app's own read-back,
because a producer and a viewer that share the same bug agree with each other.
Start this consumer, publish from the app, then stop it with Ctrl+C:

```bash
cd docker
docker compose run --rm --entrypoint python seed -c '
from confluent_kafka import Consumer
import binascii
c = Consumer({"bootstrap.servers":"kafka:19092","group.id":"verify","auto.offset.reset":"latest"})
c.subscribe(["avro-evolution"])
while True:
    m = c.poll(5.0)
    if m and not m.error():
        print(binascii.hexlify(m.value()[:5]).decode())
'
```

Run it from Git Bash or WSL, since Windows PowerShell mangles the embedded
quotes. A properly framed Avro message starts with `00` followed by a 4-byte
schema id. A message published by StreamLens today starts with `7b` (`{`),
which is the same defect `avro-unframed` simulates. Once publishing encodes
Avro, this check should show `00`.

### 10. Protobuf

`shipment-events-proto` (200 messages, 2 partitions) goes through the same
registry path. Open it as **Protobuf** and check that:

- Nested messages (`origin` and `destination`, both `GeoPoint`) expand
- The status enum shows its symbol
- The `labels` map renders as a map, with values of the right type
- The repeated field renders as an array

Two things are specific to Protobuf:

- **Message index.** Each payload carries a message index of `[1]`, because
  `GeoPoint` is declared before `ShipmentEvent` in the `.proto`. A decoder that
  only handles the single-`0x00` shortcut for the first message fails here.
- **Map fields.** The registry returns a `map<K,V>` field as a
  `repeated …Entry` field plus a nested message with
  `option map_entry = true`, rather than as `map<K,V>`. The decoder has to
  accept that form. Builds up to and including v0.6.0 don't, so on those
  builds expect this topic to fail with a decode error.

### 11. Per-topic persistence

- Set `avro-complex` to Avro, close the tab, and reopen it. It should still be
  Avro, not raw bytes.
- Restart the app. The format should still be remembered.
- A format set on one Avro topic shouldn't carry over to another.

## Resetting

The seeded topics are disposable. To start clean:

```bash
cd docker
docker compose down -v
docker compose up -d
docker compose run --rm seed python seed_avro_edge_cases.py
```

`down -v` drops the volume, so the registry loses its subjects and schema ids
start again from 1. Expect different ids from any you noted earlier.
