# StreamLens documentation

## Using StreamLens

- [User guide](usage_guide.md) - connecting to a cluster, reading and searching messages,
  publishing, exporting, consumer groups, and what the app stores on your machine
- [Masking and redaction](masking-and-redaction.md) - writing and testing redaction rules
- [Verifying your download](verification/README.md) - checking the SHA-256 hash and the code
  signature

## Testing your own scenarios

All of these run against the disposable Kafka stacks in [`docker/`](../docker/README.md).

- [Local Kafka sandbox](../docker/README.md) - what each stack starts, its ports, and the seeded
  topics
- [Connection types](testing/connections.md) - plaintext, mutual TLS, SASL/SCRAM, and an
  authenticated Schema Registry, field by field
- [Avro](testing/avro.md) - schema evolution, logical types, complex types, and malformed
  payloads
- [Kerberos](../docker/kerberos/README.md) - the GSSAPI stack, and its limits on Windows

## Release notes

One file per version in [release-notes/](release-notes/). The app's welcome screen builds its
"What's new" cards from the `## What's new` section of each file.

## Contributing

Building from source, running the tests, and the code layout are in
[CONTRIBUTING.md](../CONTRIBUTING.md).
