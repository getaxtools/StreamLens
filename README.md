# StreamLens Studio

**A fast, native desktop client for Apache Kafka.** Browse topics, tail
messages live, decode JSON and Avro, mask sensitive fields before they reach
your screen, and publish test messages — from a real desktop app, not a
browser tab. Free to use, including commercially.

> **Status: pre-release.** Windows only right now, and there are real gaps —
> see [Current limitations](#current-limitations) before you download. It
> lists exactly what does and doesn't work.

---

## Download

**[Download StreamLens.exe](https://github.com/getaxtools/StreamLens/releases/download/v0.4.0/StreamLens.exe)**
— Windows, 75 MB, single self-contained file. Or browse all builds on the
[Releases page](https://github.com/getaxtools/StreamLens/releases).

No installer, no account, no configuration — download, run, and add your
first cluster connection.

> Windows shows a SmartScreen warning on first run: the build is signed, but
> with a self-signed certificate rather than one from a certificate authority,
> so Windows can't vouch for it. Choose **More info → Run anyway** if you're
> happy to proceed. To confirm your download is the file that was published,
> check its SHA-256 hash against the release notes — see
> [Verifying your download](docs/verification/README.md).

New here? The [user guide](docs/usage_guide.md) walks through connecting to a
cluster and reading your first messages.

---

## Why another Kafka client

Most Kafka UIs are web apps you have to deploy and maintain, and the desktop
alternatives tend to be either licensed per seat or built on aging Java
toolkits. StreamLens Studio is a native Windows app that starts fast,
remembers how you work, and is free to use, including at work.

Three things it does that free alternatives generally don't:

- **Masking rules applied before display or export.** Pattern or field-path
  rules redact sensitive values in the message list, the detail pane, and
  every export. Rules fail *closed* — if a rule can't be applied, the value
  is withheld rather than shown. This is display and export hygiene, **not**
  a security boundary (the full message still comes from the broker), but it
  keeps card numbers off a screen you're sharing.
- **Production guardrails.** Tag a cluster `Production` and destructive
  actions require typing the resource name to confirm. Every change —
  connections, topic create/delete, messages published — is recorded to a
  local audit log.
- **Per-topic settings that persist.** Format and start position are
  remembered per topic, so reopening one doesn't silently reset it to raw
  bytes and start-from-beginning.

## Features

| Area | What you get |
|---|---|
| **Clusters** | Multiple saved connections, SASL/SSL, connection test, import/export. Passwords go to the Windows credential store, never to the app's database. |
| **Topics** | Live tail across all partitions, topic filter, favorites, groups, config and per-partition views with under-replication flagged |
| **Decoding** | String, JSON with pretty-printing, and Avro via Confluent Schema Registry |
| **Search** | Text scan, field paths (`$.order.id`), and timestamp ranges — across the whole topic, not just what's loaded |
| **Producing** | Compose a message with key, value, headers, and target partition — or edit and republish an existing one. Validated against the registered schema before it's sent. |
| **Masking** | Pattern / field-path redaction rules, applied everywhere the value is shown or exported |
| **Export** | CSV, JSON, and a zip of per-message JSON files, with spreadsheet formula-injection escaping |
| **Brokers & groups** | Broker list; consumer groups with state, members, and per-partition committed offsets |

## Current limitations

Being upfront so you don't waste a download:

- **Windows only.** The app won't start on macOS or Linux.
- **No consumer-group lag and no offset reset.** Groups show state, members,
  and committed offsets, but lag isn't calculated and offsets can't be
  changed from the app.
- **Protobuf messages aren't decoded.** String, JSON, and Avro work.
- **No Kafka Connect, ksqlDB, or ACL management.**
- **The audit log has no viewer yet** — it's recorded, but you can't browse
  it in the app.
- **Single user.** No logins or roles of its own; your Windows account is the
  access boundary.

## Where your data lives

Everything is local. No account, no cloud service, no telemetry.

```
%LocalAppData%\StreamLensStudio\
  streamlens.db     # connections, settings, per-topic preferences, audit log
  secrets\          # your cluster passwords, encrypted by Windows
  diagnostic.log    # local troubleshooting log
```

Kafka messages are **never** written to disk. They stream from the broker and
are held in memory only, capped at 5,000 messages per topic view.

`diagnostic.log` is plain text and records previews of message content and
the addresses of clusters you connect to (never passwords) — read
[Your data and security](docs/usage_guide.md#8-your-data-and-security) before
attaching it to a bug report.

## Documentation

- [User guide](docs/usage_guide.md) — connecting, reading messages,
  searching, publishing, exporting, and what the app stores on your machine
- [Masking and redaction](docs/usageGuide/masking-and-redaction.md) — how to
  write and test redaction rules
- [Verifying your download](docs/verification/README.md) — checking the
  SHA-256 hash, and what the code signature does and doesn't prove
- [Local Kafka sandbox](docker/README.md) — a disposable single-node cluster
  with seeded topics, for trying the app out

## Bugs and feature requests

Please open a [GitHub issue](https://github.com/getaxtools/StreamLens/issues).

For a bug, include what you were doing, what you expected, and what happened
— plus your Kafka setup (managed service or self-hosted, and which security
protocol) if it's a connection problem.

Feature requests are genuinely useful at this stage: the roadmap isn't fixed,
and what people actually run into shapes what gets built next.

## License

StreamLens Studio is **free to use, including commercially**, on as many
machines as you like — no seat limits, no registration, no trial period.

It is proprietary software: the source code isn't published, and the app may
not be redistributed or resold. Full terms in [LICENSE](LICENSE).

StreamLens Studio bundles third-party open-source components; their licenses
are reproduced in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
