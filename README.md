# StreamLens Studio

**A fast, native desktop client for Apache Kafka.** Browse topics, tail
messages live, decode JSON, Avro and Protobuf, watch consumer-group lag, mask
sensitive fields before they reach your screen, and publish test messages, all
from a real desktop app rather than a browser tab. Free to use, including
commercially.

> **Status: pre-release.** Windows only right now, and there are real gaps.
> Have a look at [Current limitations](#current-limitations) before you
> download, since it lists exactly what does and doesn't work.

---

## Download

**[Download StreamLens.exe](https://github.com/getaxtools/StreamLens/releases/download/0.7.0/StreamLens.exe)**
for Windows. It's a single self-contained file, about 75 MB. You can also
browse all the builds on the
[Releases page](https://github.com/getaxtools/StreamLens/releases).

No installer, no account, nothing to configure. Download it, run it, and add
your first cluster connection.

> Windows shows a SmartScreen warning on first run. The build is signed, but
> with a self-signed certificate rather than one from a certificate authority,
> so Windows has no way to vouch for it. Choose **More info → Run anyway** if
> you're happy to proceed. To confirm your download really is the file that
> was published, check its SHA-256 hash against the release notes. There's a
> walkthrough in [Verifying your download](docs/verification/README.md).

New here? The [user guide](docs/usage_guide.md) walks you through connecting
to a cluster and reading your first messages.

---

## Why another Kafka client

Most Kafka UIs are web apps you have to deploy and maintain, and the desktop
alternatives tend to be either licensed per seat or built on aging Java
toolkits. StreamLens Studio is a native Windows app that starts fast,
remembers how you work, and is free to use, including at work.

Three things it does that free alternatives generally don't:

- **Masking rules applied before display or export.** Pattern or field-path
  rules redact sensitive values in the message list, the detail pane, and
  every export. Rules fail *closed*, so if one can't be applied the value is
  withheld rather than shown. Treat this as display and export hygiene rather
  than a security boundary, because the full message still comes from the
  broker. What it does do is keep card numbers off a screen you're sharing.
- **Production guardrails, with a log you can read.** Tag a cluster
  `Production` and destructive actions make you type the resource name to
  confirm. Every change gets recorded to a local audit log covering
  connections, topic creates and deletes, offset moves, and messages
  published, and **Tools → Audit Log…** lets you filter and export it.
- **Per-topic settings that persist.** Format and start position are
  remembered per topic, so reopening one doesn't quietly reset it to raw bytes
  and start-from-beginning.

## Features

| Area | What you get |
|---|---|
| **Clusters** | Multiple saved connections, SASL/SSL, connection test, import/export. Passwords go to the Windows credential store, never to the app's database. |
| **Topics** | Live tail across all partitions, topic filter, favorites, groups, config and per-partition views with under-replication flagged |
| **Decoding** | String, JSON with pretty-printing, and Avro and Protobuf via Confluent Schema Registry |
| **Search** | Text scan, field paths (`$.order.id`), and timestamp ranges, searched across the whole topic rather than just what's loaded |
| **Producing** | Compose a message with key, value, headers, and target partition, or edit and republish an existing one. Values are sent as plain text, and a JSON value is checked against the topic's registered Avro schema before it's sent. |
| **Masking** | Pattern and field-path redaction rules, applied everywhere the value is shown or exported |
| **Export** | CSV, JSON, and a zip of per-message JSON files, with spreadsheet formula-injection escaping |
| **Brokers & groups** | Broker list, plus consumer groups with state, members, per-partition lag, and offset editing |
| **Offsets** | Move a group per partition, send every partition to the start or the end, or reset the whole topic to earliest, latest, or a timestamp |
| **Audit log** | A viewer for every write and destructive action, filterable by cluster, action, or text, with CSV export |
| **Diagnostics** | An Output panel showing each connection step and refresh as it happens, with timings, so a slow step doesn't look like a hang |
| **Bug reports** | Crashes and failed connections write a report you can file as a pre-filled GitHub issue, after a preview showing exactly what it contains |
| **Updates** | Tells you when a new version is out and links straight to the download. Checks once a day, sends nothing about you, and stays quiet when you're offline |

## Current limitations

Being upfront so you don't waste a download:

- **Windows only.** The app won't start on macOS or Linux.
- **No Kafka Connect, ksqlDB, or ACL management.**
- **Publishing doesn't encode Avro or Protobuf.** The app sends the value as
  plain UTF-8 text, without the Confluent framing (magic byte and schema ID).
  Anything you publish to an Avro or Protobuf topic can't be read by consumers
  that use the registered schema, so use your own producer for those topics.
- **Protobuf needs a Schema Registry.** A payload with no registered schema
  can't be decoded, since there's no schemaless fallback.
- **Offsets move one topic at a time**, and only while the group has no
  running members. Kafka refuses it otherwise, so the app refuses first.
- **Kerberos (GSSAPI) is configurable but not verified end to end on
  Windows.** The broker setup has been tested against a test KDC with a Linux
  Kafka client, but the app itself runs only on Windows, where it uses SSPI.
  That path needs a domain-joined machine and an Active Directory service
  principal, which we haven't tested. Keytabs don't work on
  Windows at all, since the underlying client uses your logged-on account.
- **Single user.** No logins or roles of its own, so your Windows account is
  the access boundary.

## What's coming next

The roadmap isn't fixed, and what people report shapes the order things get
built in. Roughly what's on the list at the moment, most-wanted first:

- **macOS and Linux builds.** The UI toolkit is cross-platform already, so
  the work here is in the platform-specific pieces, mainly the credential
  store, which is Windows DPAPI today.
- **Protobuf without a Schema Registry**, by pointing the app at a descriptor
  file instead. Registry-backed decoding works today; a local `.proto` doesn't.
- **Verifying Kerberos on Windows** against a domain-joined machine and a real
  Active Directory service principal.
- **Moving offsets for more than one topic at a time**, which is the obvious
  next step now that the per-topic case is in.
- **A CA-issued code-signing certificate**, which would put an end to the
  SmartScreen warning on every download.

Want something that isn't on this list, or think the order is wrong? Say so in
an [issue](https://github.com/getaxtools/StreamLens/issues). At this stage a
single well-argued request genuinely does move things around.

## Where your data lives

Everything is local. No account, no cloud service, no telemetry.

The one exception is the update check. Once a day StreamLens asks GitHub what
the latest released version is. It sends nothing about you, your machine, or
your clusters, and it does nothing at all when you're offline. There's more
detail in
[Keeping StreamLens up to date](docs/usage_guide.md#11-keeping-streamlens-up-to-date).

```
%LocalAppData%\StreamLensStudio\
  streamlens.db     # connections, settings, per-topic preferences, audit log
  secrets\          # your cluster passwords, encrypted by Windows
  diagnostic.log    # local troubleshooting log
  crashes\          # crash reports, if the app has ever hit one
  update-check.json # when updates were last checked for, and any version you dismissed
  window-layout.json # window chrome that survives a restart, such as the sidebar width
```

Kafka messages are **never** written to disk. They stream from the broker and
are held in memory only, capped at 5,000 messages per topic view.

`diagnostic.log` is plain text, and it records previews of message content
along with the addresses of clusters you connect to (never passwords). Read
[Your data and security](docs/usage_guide.md#9-your-data-and-security) before
you attach it to a bug report. The same goes for a crash report, which is why
the app shows you one in full before it goes anywhere.

## Documentation

- [User guide](docs/usage_guide.md) covers connecting, reading messages,
  searching, publishing, exporting, and what the app stores on your machine
- [Masking and redaction](docs/usageGuide/masking-and-redaction.md) explains
  how to write and test redaction rules
- [Verifying your download](docs/verification/README.md) walks through
  checking the SHA-256 hash, and what the code signature does and doesn't
  prove
- [Local Kafka sandbox](docker/README.md) gives you a disposable single-node
  cluster with seeded topics, for trying the app out
- [Testing connection types](docs/testing-connections.md) has step-by-step
  setup for plaintext, mutual TLS, SASL/SCRAM, and an authenticated Schema
  Registry

## Bugs and feature requests

Please open a [GitHub issue](https://github.com/getaxtools/StreamLens/issues).

For a bug, include what you were doing, what you expected, and what actually
happened. If it's a connection problem, your Kafka setup helps too: managed
service or self-hosted, and which security protocol.

After a crash or a failed connection, the **Report this** button in the app
does most of that for you. It puts the report on your clipboard and opens a
pre-filled issue, and you get to read the whole thing before any of it leaves
your machine.

Feature requests are genuinely useful at this stage. The roadmap isn't fixed,
and what people actually run into shapes what gets built next.

## License

StreamLens Studio is **free to use, including commercially**, on as many
machines as you like. No seat limits, no registration, no trial period.

It is proprietary software: the source code isn't published, and the app may
not be redistributed or resold. Full terms are in [LICENSE](LICENSE).

StreamLens Studio bundles third-party open-source components, and their
licenses are reproduced in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
