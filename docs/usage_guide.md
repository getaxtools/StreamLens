# StreamLens Studio — User Guide

**A desktop app for looking inside Apache Kafka.** Browse topics, watch
messages arrive live, search them, hide sensitive fields, and publish test
messages — without writing a line of code.

This guide is for using the app. If you want to build it from source or
contribute, see the [README](../README.md).

---

## Contents

1. [Installing](#1-installing)
2. [Connecting to a cluster](#2-connecting-to-a-cluster)
3. [Reading messages](#3-reading-messages)
4. [Finding a specific message](#4-finding-a-specific-message)
5. [Publishing a message](#5-publishing-a-message)
6. [Exporting messages](#6-exporting-messages)
7. [Hiding sensitive data](#7-hiding-sensitive-data)
8. [Your data and security](#8-your-data-and-security)
9. [Settings](#9-settings)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Installing

**You need Windows.** macOS and Linux aren't supported yet — the app won't
start on them.

Pre-built installers aren't published yet, so for now the app is built from
source. Ask whoever set it up in your team, or follow the build steps in the
[README](../README.md).

**Nothing to configure and no account to create.** StreamLens runs entirely
on your machine and talks only to the Kafka clusters you point it at.

---

## 2. Connecting to a cluster

You'll need these from whoever runs your Kafka cluster:

- The **bootstrap servers** address — e.g. `kafka.example.com:9092`
- The **security settings** — whether it needs a username and password, and
  whether the connection is encrypted
- Optionally a **Schema Registry URL**, if your team uses Avro

### Adding a connection

1. Click **Add Cluster**.
2. Fill in:

   | Field | What to put |
   |---|---|
   | **Name** | Anything that helps you recognise it — `Prod EU`, `Team Staging` |
   | **Bootstrap Servers** | The address you were given, e.g. `kafka.example.com:9092` |
   | **Environment** | `Local`, `Development`, `Staging`, or `Production` — see the warning below |
   | **Security Protocol** | `Plaintext` for a local test broker; otherwise ask your Kafka admin |
   | **SASL Mechanism** / **Username** | Only if your cluster requires a login |
   | **Schema Registry URL** | Optional — only if your team uses Avro |

3. Click **Connect**. The topic list fills in on the left.

> ### Set **Environment** correctly — especially for production
>
> Tagging a cluster as **Production** turns on extra safety: anything
> destructive makes you type the resource name to confirm, so you can't
> delete the wrong topic with a stray click. It costs you nothing on a
> cluster you're just browsing, and it's the difference between a near-miss
> and an incident. **Set it when you create the connection** — that's the
> moment you know which cluster this is.

> ### Check the Security Protocol before connecting to a real cluster
>
> New connections default to **Plaintext**, which sends your traffic
> unencrypted. That's fine for a test broker on your own machine. For any
> shared, staging, or production cluster, ask your Kafka admin for the right
> setting (usually `Ssl` or `SaslSsl`) — don't leave it on the default.

**Your password is not stored in the app's database.** It goes into Windows'
built-in credential encryption, tied to your Windows account. See
[Section 8](#8-your-data-and-security).

### Managing connections

Connections are saved, so you set them up once. Under **File** you can
**Export Connections…** to share your setup with a colleague, or **Import
Connections…** to load theirs.

**Exported connection files do not contain passwords** — whoever imports one
enters their own credentials. That's deliberate: it means a connection file
is safe to send over chat or email.

---

## 3. Reading messages

Click any topic in the left-hand list and messages start streaming in.

### Making messages readable

Raw Kafka messages are just bytes. The **Format** dropdown decides how
they're displayed:

| Format | Use it when |
|---|---|
| **String** | The message is plain text |
| **Json** | The message is JSON — this pretty-prints it, indented and readable |
| **Avro** | Your team uses Avro (needs the Schema Registry URL on the connection) |
| **Hex** | You're debugging binary data and want to see the actual bytes |
| **Raw** | You want no interpretation at all |

**StreamLens remembers this per topic.** Set `order-created` to `Json` once
and it opens as `Json` every time after — you never re-pick it.

If a message won't decode, you'll see an error in place of the value rather
than a crash. For Avro, a message that isn't in the expected format says so
specifically — usually that means the topic isn't really Avro.

### Where the stream starts

**Default start position** controls where reading begins:

- **Latest** — the most recent messages, then new ones as they arrive. This
  is what you want for watching live traffic.
- **Earliest** — replays the topic from the beginning. Use this to
  investigate something that already happened.

This is also remembered per topic.

### Other views

Beyond the message list, each topic has **Config** (its Kafka settings) and
**Partitions** (per-partition detail, with under-replicated partitions
flagged). The cluster itself has **BROKERS** and **CONSUMERS** views —
consumer groups show their state, members, and committed offsets.

### Staying organised

- **Star a topic** to pin it to **FAVORITES** at the top of the list.
- **Filter topics…** narrows a long list as you type.
- Topics can be sorted into **groups**, and given colours.

> **Note:** the message list holds the most recent 5,000 messages. Older ones
> drop off as new ones arrive. Messages are never saved to your disk — see
> [Section 8](#8-your-data-and-security).

---

## 4. Finding a specific message

Three ways, depending on what you know:

**Search the messages on screen** — type in the search box to filter the
loaded list by key, value, or headers.

**Search inside one message** — with a message selected, search its content,
or type a path like `$.customer.email` to jump to one field.

**Search the whole topic** (**Tools → Find Messages…**) — scans the topic on
the broker, not just what's loaded. Use this when the message you want is
older than what's on screen. You can search by:

- **Text** — any message containing your term
- **Field path** — e.g. `$.order.id` to match a specific JSON field
- **Time range** — everything between two timestamps

Use **Stop** to end a long scan early; results found so far are kept.

---

## 5. Publishing a message

Useful for testing a consumer or reproducing a bug.

**From scratch:** click **New**, fill in the value (and optionally a key,
headers, and a target partition), then **Publish**.

**From an existing message:** select a message, choose **Republish**, and
edit it before sending. Easier than retyping a realistic message.

If your topic has a registered Avro schema, StreamLens **checks your message
against it before sending** and tells you what's wrong rather than letting a
malformed message onto the topic.

> **On a Production-tagged cluster, you'll be asked to confirm by typing the
> resource name.** This is intentional friction — you're writing to
> production.

Every message you publish is recorded in a local audit log
([Section 8](#8-your-data-and-security)).

---

## 6. Exporting messages

Under **File**:

- **Export Messages as CSV…** — for Excel or Google Sheets
- **Export Messages as JSON…** — for scripts and other tools
- **Download as ZIP…** — one JSON file per message

**Two things worth knowing before you send an export to anyone:**

1. **Masking rules apply to exports.** Fields hidden on screen are hidden in
   the file too ([Section 7](#7-hiding-sensitive-data)).
2. **CSV exports are protected against spreadsheet formula injection.** A
   message value starting with `=` can't turn into a live formula when the
   file is opened in Excel.

---

## 7. Hiding sensitive data

If your messages carry personal data — SSNs, card numbers, emails — you can
have StreamLens hide those fields automatically.

**Tools → Masking Rules… → New rule.** A rule needs a name, a pattern (either
a text pattern or a field path like `$.customer.ssn`), and what to replace
matches with. Leave the topic blank to apply it everywhere.

Once saved, matching values are hidden **in the message list, in the detail
pane, and in every export** — you set the rule once and it applies
everywhere.

Two behaviours worth knowing:

- **If a rule can't be applied, the value is hidden rather than shown.** The
  app fails safe: you'll never see real data because a rule quietly failed.
- **A rule with a mistake in it is reported, not ignored.** You'll know it
  isn't working.

> ### Masking hides data on your screen. It does not secure it.
>
> The full message is still fetched from Kafka — masking only changes what's
> displayed and exported. It **does** stop a customer's SSN appearing in a
> screen-share, a screenshot, or a CSV you email. It **does not** stop anyone
> with their own Kafka access from reading the original message.
>
> For real access control, you need Kafka ACLs from your cluster admin.
> Masking sits alongside those, not instead of them.

Full details, including pattern examples: [Masking & redaction
rules](usageGuide/masking-and-redaction.md).

---

## 8. Your data and security

### Everything stays on your machine

**No account, no cloud service, no telemetry.** StreamLens talks to your
Kafka clusters and nothing else. Nobody — including the people who make it —
can see your data or your connections.

### Kafka messages are never saved to disk

Messages stream from the broker straight into memory and are dropped when you
close the topic. Nothing is cached, and closing the app leaves no copy of
your message data behind.

The exceptions are the ones you create yourself: a file you **export**
(Section 6), and the diagnostic log described below.

### What is saved

In `%LocalAppData%\StreamLensStudio\`:

| What | Contains |
|---|---|
| `streamlens.db` | Your connections, settings, per-topic preferences, masking rules, and the audit log |
| `secrets\` | Your cluster passwords, encrypted by Windows |
| `diagnostic.log` | A troubleshooting log |

Deleting that folder resets the app to a fresh install.

### How your passwords are protected

Cluster passwords go into Windows' built-in credential encryption (DPAPI),
tied to your Windows user account. They are **never** written to the app's
database, and never appear in exported connection files or in any log.

### The audit log

Every change is recorded locally — connections created or deleted, topics
created or deleted, messages published. It's a record of what was done from
your machine, useful if you need to reconstruct what happened. It's stored in
the database; there's no viewer for it in the app yet.

### Before you share `diagnostic.log`

The diagnostic log is **plain text** and records **previews of message
content** and the **addresses of clusters you connect to**. It never contains
passwords.

If you're attaching it to a bug report or sending it to support, open it
first and remove anything sensitive. It's there to help you diagnose a
problem locally — treat it as you'd treat the messages themselves.

### Honest limits

So you can judge the risk for yourself:

- **Your saved settings file is not encrypted.** Connection names, addresses,
  and topic preferences sit in a plain database file. Your passwords are
  encrypted; this other information isn't. Anyone who can read files in your
  Windows account can read it.
- **Your Windows account is the only boundary.** StreamLens has no separate
  login, no user roles, and no permissions of its own. Anyone who can use
  your unlocked Windows account can use your saved connections. Lock your
  screen.
- **Masking is not access control** — see [Section 7](#7-hiding-sensitive-data).
- **New connections default to unencrypted transport** — see
  [Section 2](#2-connecting-to-a-cluster).

### What StreamLens can't do to your cluster

It can't change consumer group offsets, modify ACLs, or alter cluster
configuration — none of that is implemented. It reads, and it publishes
messages and creates or deletes topics when you ask it to.

---

## 9. Settings

**Tools → Settings…** covers app-wide defaults: your default format and start
position for topics you haven't opened before, and whether timestamps display
in UTC or local time.

You can keep **multiple settings profiles** and switch between them — handy
if you work across teams whose topics use different formats. Mark one as the
default.

---

## 10. Troubleshooting

| What you see | What it usually means |
|---|---|
| **The app won't start** | You're on macOS or Linux. Windows only for now. |
| **The topic list is empty after connecting** | Check the bootstrap address for typos, and that you can reach the cluster from your network — a VPN is often the missing piece. |
| **A topic shows no messages, but you know it has data** | Start position is **Latest**, which only shows recent activity. Switch to **Earliest** to replay from the beginning. |
| **Messages look like unreadable symbols** | Wrong **Format**. Try `Json` or `String`. |
| **Avro messages won't decode** | Either the Schema Registry URL is missing from the connection, or the topic isn't actually Avro — the error message distinguishes these. |
| **Connection fails on a secured cluster** | The **Security Protocol** or **SASL Mechanism** doesn't match what the cluster expects. Confirm both with your Kafka admin. Never work around a certificate error by disabling verification — that defeats the encryption. |
| **A field you masked is still visible** | Check the rule is enabled and its topic field either matches or is blank. Test the pattern in the rules dialog. |
| **You can't delete a topic without typing its name** | Working as intended — the cluster is tagged **Production**. |

**Still stuck?** Open an issue on the project's GitHub repository. Include
what you were doing, what you expected, and what happened. If you attach
`diagnostic.log`, read the warning in [Section 8](#8-your-data-and-security)
first.
