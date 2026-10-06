# StreamLens Studio User Guide

**A desktop app for looking inside Apache Kafka.** Browse topics, watch
messages arrive live, search them, manage consumer-group offsets, hide
sensitive fields, and publish test messages, without writing a line of code.

This guide is for using the app. For downloads, feature overview, and known
limitations, see the [README](../README.md).

---

## Contents

1. [Installing](#1-installing)
2. [Connecting to a cluster](#2-connecting-to-a-cluster)
3. [Reading messages](#3-reading-messages)
4. [Finding a specific message](#4-finding-a-specific-message)
5. [Publishing a message](#5-publishing-a-message)
6. [Consumer groups, lag, and offsets](#6-consumer-groups-lag-and-offsets)
7. [Exporting messages](#7-exporting-messages)
8. [Hiding sensitive data](#8-hiding-sensitive-data)
9. [Your data and security](#9-your-data-and-security)
10. [The audit log](#10-the-audit-log)
11. [Keeping StreamLens up to date](#11-keeping-streamlens-up-to-date)
12. [When something goes wrong](#12-when-something-goes-wrong)
13. [Settings](#13-settings)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Installing

**You need Windows.** macOS and Linux aren't supported yet, and the app won't
start on them.

**[Download StreamLens.exe](https://github.com/getaxtools/StreamLens/releases/latest)**
from the Releases page. It's a single self-contained file with no installer:
save it anywhere and run it.

Windows shows a SmartScreen warning on first run, because the build isn't
signed by a certificate authority. Choose **More info -> Run anyway** if you're
happy to proceed. To confirm your download is the published file, check its
SHA-256 hash against the release notes. See
[Verifying your download](verification/README.md).

**Nothing to configure and no account to create.** StreamLens runs entirely
on your machine. The only things it contacts are the Kafka clusters you point
it at, and GitHub, once a day, to see whether a new version has been
released. That check sends nothing about you, and is covered in
[Section 11](#11-keeping-streamlens-up-to-date).

**The first thing you'll see is the welcome screen**: a **Connect** button
sitting between two cards, one covering what the app can do and one with a
rotating tip naming where each feature lives in the menus. They're worth a
read before you go hunting through the menus yourself. Both cards disappear
once you open a connection.

---

## 2. Connecting to a cluster

You'll need these from whoever runs your Kafka cluster:

- The **bootstrap servers** address, e.g. `kafka.example.com:9092`
- The **security settings**, meaning whether it needs a username and password,
  and whether the connection is encrypted
- Optionally a **Schema Registry URL**, if your team uses Avro or Protobuf

### Adding a connection

1. Open **File → Add Connection…**.
2. Fill in:

   | Field | What to put |
   |---|---|
   | **Name** | Anything that helps you recognise it, such as `Prod EU` or `Team Staging` |
   | **Bootstrap Servers** | The address you were given, e.g. `kafka.example.com:9092` |
   | **Environment** | `Local`, `Development`, `Staging`, or `Production`. See the warning below |
   | **Security Protocol** | `Plaintext` for a local test broker; otherwise ask your Kafka admin |
   | **SASL Mechanism** / **Username** | Only if your cluster requires a login |
   | **Schema Registry URL** | Optional, only if your team uses Avro or Protobuf |

   Everything past the basics lives under **Advanced options**, collapsed
   until you need it.

3. Click **Connect**. The topic list fills in on the left.

> ### Set **Environment** correctly, especially for production
>
> Tagging a cluster as **Production** turns on extra safety: anything
> destructive makes you type the resource name to confirm, so you can't
> delete the wrong topic with a stray click. It costs you nothing on a
> cluster you're just browsing, and it's the difference between a near-miss
> and an incident. **Set it when you create the connection**, since that's the
> moment you know which cluster this is.

> ### Check the Security Protocol before connecting to a real cluster
>
> New connections default to **Plaintext**, which sends your traffic
> unencrypted. That's fine for a test broker on your own machine. For any
> shared, staging, or production cluster, ask your Kafka admin for the right
> setting (usually `Ssl` or `SaslSsl`) and don't leave it on the default.

**Your password is not stored in the app's database.** It goes into Windows'
built-in credential encryption, tied to your Windows account. See
[Section 9](#9-your-data-and-security).

### If your cluster uses TLS

Most company clusters do. Choose `Ssl` or `SaslSsl` as the **Security
Protocol** and a few more fields appear. They're hidden the rest of the
time, because on an unencrypted connection they'd do nothing.

What you need depends on how your cluster is set up, and your Kafka admin
will know which of these applies:

**The broker's certificate comes from your company's own authority.** Common
in anything self-hosted. Windows doesn't trust that authority out of the box,
so the connection fails until you point the **CA certificate** field at the
CA file your admin gives you.

**The broker wants a certificate from you too.** This is mutual TLS, and the
cluster won't talk to a client it can't identify. You'll be given a client
certificate and a key file; fill in both. If the key has a passphrase, it
goes in the Windows credential store rather than the connection file.

**It's a dev broker you set up yourself and there's no CA file anywhere.**
There's a **Skip certificate verification** checkbox for exactly this. It
does what it says: nothing is checked, so nothing is really protected. Fine
against a container on your laptop, never against anything else.

### If your Schema Registry needs a login

The registry is a separate service from the broker, and it has its own login.
Filling in the SASL username and password doesn't cover it, because that's the
broker's login. There are separate **Schema Registry Username** and
**Password** fields underneath the registry URL for this.

Confluent Cloud works this way: the broker authenticates with SASL, the
registry with a plain username and password. If the registry sits behind a
private CA as well, it uses the same CA certificate you set above.

Get this wrong and the symptom is confusing. The cluster connects fine and
topics list normally, but Avro and Protobuf messages refuse to decode.

### If your cluster uses Kerberos

Choose **GSSAPI** as the SASL mechanism and three more fields appear:

| Field | What to put |
|---|---|
| **Kerberos service name** | The service principal's first part, the `kafka` in `kafka/host@REALM`. Ask your admin; `kafka` is the usual answer. |
| **Kerberos principal** | Who you authenticate as, e.g. `alice@EXAMPLE.COM`. Leave it blank to use whichever ticket you already have. |
| **Kerberos keytab** | A keytab file to get a ticket from, for an unattended login. |

**On Windows, Kerberos uses your logged-on account.** The underlying Kafka
client authenticates through Windows' own SSPI, so you get a ticket by being
logged in to the domain, not by pointing the app at a file. **Keytabs do
nothing on Windows**, and the form says so rather than pretending otherwise.

This path is configurable but not verified end to end on Windows, which needs
a domain-joined machine and an Active Directory service principal. If you try
it, an issue saying whether it worked is genuinely useful.

### Managing connections

Set a connection up once and it's saved. Everything to do with them sits in
the **File** menu. **Connect to** lists the clusters you've saved, with
**Add**, **Edit**, and **Delete Connection…** below it.

Working with someone else on the same clusters? **Export Connections…**
writes your setup to a file they can pick up with **Import Connections…**.

**Exported connection files do not contain passwords**, so whoever imports one
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
| **Json** | The message is JSON. It's pretty-printed, indented and readable |
| **Avro** | Your team uses Avro (needs the Schema Registry URL on the connection) |
| **Protobuf** | Your team uses Protobuf (also needs the Schema Registry URL) |
| **Hex** | You're debugging binary data and want to see the actual bytes |
| **Raw** | You want no interpretation at all |

**StreamLens remembers this per topic.** Set `order-created` to `Json` once
and it opens as `Json` every time after, so you never re-pick it.

If a message won't decode, you'll see an error in place of the value rather
than a crash. For Avro and Protobuf, a message that isn't in the expected
format says so specifically, which usually means the topic isn't really in
that format.

**Protobuf is decoded against the `.proto` your Schema Registry holds.** Field
names come from the schema rather than being numbered, enums read as their
names, nested messages and maps render as nested JSON, and byte fields show as
hex. The result is real JSON, so field-path search works against it. There's
no schemaless fallback: a payload with no registered schema can't be decoded.

### Where the stream starts

**Default start position** controls where reading begins:

- **Latest** shows the most recent messages, then new ones as they arrive.
  This is what you want for watching live traffic.
- **Earliest** replays the topic from the beginning. Use this to
  investigate something that already happened.

This is also remembered per topic.

### Reloading a topic

**Reload**, on the Messages toolbar, re-reads the topic from the current start
position and replaces what's on screen. Use it when a topic has moved on and
you want to see where it's got to, without closing and reopening the tab. The
button is disabled while a reload is running.

### Other views

Beyond the message list, each topic has **Config** (its Kafka settings) and
**Partitions** (per-partition detail, with under-replicated partitions
flagged).

The connection's **Home** tab is the cluster overview: the broker list, the
cluster's own details, and its default settings. It's what you land on when
you connect and whenever no topic tab is open. Consumer groups live in the
**CONSUMERS** section of the sidebar, covered in
[Section 6](#6-consumer-groups-lag-and-offsets).

### Staying organised

- **Star a topic** to pin it to **FAVORITES** at the top of the list.
- **Filter topics…** narrows a long list as you type.
- Topics can be sorted into **groups**, and given colours.

> **Note:** the message list holds the most recent 5,000 messages. Older ones
> drop off as new ones arrive. Messages are never saved to your disk. See
> [Section 9](#9-your-data-and-security).

---

## 4. Finding a specific message

Three ways, depending on what you know:

**Search the messages on screen.** Type in the search box to filter the
loaded list by key, value, or headers.

**Search inside one message.** With a message selected, search its content,
or type a path like `$.customer.email` to jump to one field.

**Search the whole topic** (**Tools → Find Messages…**) scans the topic on
the broker, not just what's loaded. Use this when the message you want is
older than what's on screen. You can search by:

- **Text**: any message containing your term
- **Field path**: e.g. `$.order.id` to match a specific JSON field
- **Time range**: everything between two timestamps

Use **Stop** to end a long scan early; results found so far are kept.

---

## 5. Publishing a message

Useful for testing a consumer or reproducing a bug.

**From scratch:** click **New**, fill in the value (and optionally a key,
headers, and a target partition), then **Publish**.

**From an existing message:** select a message, choose **Republish**, and
edit it before sending. Easier than retyping a realistic message.

If your topic has a registered Avro schema, StreamLens **checks your JSON
against it before sending** and tells you what's wrong. That check is about
the message's shape only. It doesn't change how the message is sent (see
below).

> ### Don't publish to an Avro or Protobuf topic from StreamLens
>
> StreamLens sends exactly the text in the editor, encoded as UTF-8. It does
> not encode the message as Avro or Protobuf, and it does not add the Confluent
> framing (a magic byte plus a 4-byte schema ID) those consumers expect. That's
> true of **New** and **Republish** alike, and whatever format you're viewing
> the topic in.
>
> So anything you publish to a schema-backed topic lands as plain text that
> schema-aware consumers can't read. Other Kafka tools report it as a serde
> fallback. A record can't be un-produced, so use your own producer for those
> topics until StreamLens can encode them.

> **On a Production-tagged cluster, you'll be asked to confirm by typing the
> resource name.** This is intentional friction, because you're writing to
> production.

Every message you publish is recorded in a local audit log
([Section 10](#10-the-audit-log)).

---

## 6. Consumer groups, lag, and offsets

Click a group under **CONSUMERS** in the sidebar and its detail dialog opens.

### Seeing how far behind a group is

**MEMBERS** lists the consumers currently in the group. **PARTITION OFFSETS**
is the part you're usually here for, with one row per partition:

| Column | What it means |
|---|---|
| **Offset** | Where the group has committed to |
| **End** | The end of the log, the newest message there |
| **Lag** | The distance between them, so how far behind the group is |

The group's **total lag** sits in the header. A partition that has never been
committed to reads `—` rather than a number, because there's no offset to
measure from and a figure there would be invented.

### Moving a group's offsets

Three ways, in the same dialog:

**Edit one partition.** Type a new value into its **Offset** cell. End and Lag
update as you type, so you can see where the group will land before you commit
to anything. Values are clamped to that partition's real range, so typing
99999999 gives you the end of the log instead of a broker error.

**Move everything at once.** **⏮ To start** and **To end ⏭** send every
partition to its own start or end. Those differ per partition, which is why
one typed number wouldn't do.

**Reset the whole topic.** **Reset offsets…** covers the earliest retained
message, the end of the log, or a timestamp, which is the "replay everything
since 14:30" case during an incident.

**Undo edits** puts every partition back to what the broker holds. Nothing
reaches Kafka until you click **Apply offsets**, and applying commits **only
the partitions you changed**.

> ### Two things that will stop you, both on purpose
>
> **A group with running consumers can't be moved.** Kafka refuses it, so the
> app refuses first and names the consumers you need to stop, rather than
> letting you fill in the dialog and collect an error at the end.
>
> **On a Production-tagged cluster, you'll be asked to type the resource name
> to confirm.** Same guardrail as deleting a topic. The previous offsets
> aren't recoverable, which is exactly why it's behind a confirmation.

Every move lands in the audit log ([Section 10](#10-the-audit-log)).

---

## 7. Exporting messages

Under **File**:

- **Export Messages as CSV…** for Excel or Google Sheets
- **Export Messages as JSON…** for scripts and other tools
- **Download as ZIP…** for one JSON file per message

**Three things worth knowing before you send an export to anyone:**

1. **Masking rules apply to exports.** Fields hidden on screen are hidden in
   the file too ([Section 8](#8-hiding-sensitive-data)).
2. **CSV exports are protected against spreadsheet formula injection.** A
   message value starting with `=` can't turn into a live formula when the
   file is opened in Excel.
3. **You export what's on screen, in the format it's displayed in.** Set
   **Format** correctly before exporting, not after.

### If your export looks like binary junk

A value full of `\u0000` escapes with fragments of readable text between them
is an **Avro or Protobuf payload exported as String**. Both start with a zero
byte followed by a 4-byte schema ID, so reading one as text gives you the
field values separated by unprintable characters:

```
\u0000\u0000\u0000\u0000\u0002\u0014CLI-100002\u0012CO-200001…
```

Set **Format** to `Avro` or `Protobuf`, confirm the values decode on screen,
then export again. If they don't decode, the Schema Registry URL is missing
or the registry needs its own login ([Section 2](#2-connecting-to-a-cluster)).

StreamLens never silently falls back to another format. A value it can't
decode reads `[decode error: …]` followed by the raw hex, so an export that
looks like the block above was taken as `String` rather than having failed to
decode.

If another Kafka tool reports the same message as a **serde fallback**, that's
a different problem: the record itself is unframed, most likely because it was
published as plain text. See the warning in
[Section 5](#5-publishing-a-message).

---

## 8. Hiding sensitive data

If your messages carry personal data such as SSNs, card numbers, or emails, you can
have StreamLens hide those fields automatically.

**Tools → Masking Rules… → New rule.** A rule needs a name, a pattern (either
a text pattern or a field path like `$.customer.ssn`), and what to replace
matches with. Leave the topic blank to apply it everywhere.

Once saved, matching values are hidden **in the message list, in the detail
pane, and in every export**. You set the rule once and it applies
everywhere.

Two behaviours worth knowing:

- **If a rule can't be applied, the value is hidden rather than shown.** The
  app fails safe: you'll never see real data because a rule quietly failed.
- **A rule with a mistake in it is reported, not ignored.** You'll know it
  isn't working.

> ### Masking hides data on your screen. It does not secure it.
>
> The full message is still fetched from Kafka, and masking only changes what's
> displayed and exported. It **does** stop a customer's SSN appearing in a
> screen-share, a screenshot, or a CSV you email. It **does not** stop anyone
> with their own Kafka access from reading the original message.
>
> For real access control, you need Kafka ACLs from your cluster admin.
> Masking sits alongside those, not instead of them.

Full details, including pattern examples: [Masking & redaction
rules](usageGuide/masking-and-redaction.md).

---

## 9. Your data and security

### Everything stays on your machine

**No account, no cloud service, no telemetry.** Nobody, including the people
who make it, can see your data or your connections.

StreamLens makes exactly one kind of request that isn't to your own Kafka
cluster: once a day it asks GitHub what the latest released version is. It
sends nothing about you or your clusters, and you can read what it does and
doesn't do in [Section 11](#11-keeping-streamlens-up-to-date).

### Kafka messages are never saved to disk

Messages stream from the broker straight into memory and are dropped when you
close the topic. Nothing is cached, and closing the app leaves no copy of
your message data behind.

The exceptions are the ones you create yourself: a file you **export**
(Section 7), the diagnostic log described below, and a crash report if the app
ever hits one ([Section 12](#12-when-something-goes-wrong)).

### What is saved

In `%LocalAppData%\StreamLensStudio\`:

| What | Contains |
|---|---|
| `streamlens.db` | Your connections, settings, per-topic preferences, masking rules, and the audit log |
| `secrets\` | Your cluster passwords, encrypted by Windows |
| `diagnostic.log` | A troubleshooting log |
| `crashes\` | Crash reports, if the app has ever hit one ([Section 12](#12-when-something-goes-wrong)) |
| `update-check.json` | When updates were last checked for, and any version you dismissed |
| `window-layout.json` | Window chrome that survives a restart, such as the sidebar width |

Deleting that folder resets the app to a fresh install.

### How your passwords are protected

Every secret the app holds goes to the same place: Windows' built-in
credential encryption (DPAPI), tied to your Windows account. That covers
cluster passwords, the passphrase on a certificate key, and your Schema
Registry password. None of them are **ever** written to the app's database,
and none appear in an exported connection file or in any log.

Certificates are a slightly different case, because they're files you already
have on disk. The app remembers *where* they are, and reads them when it
connects, and it never copies their contents into its own database.

### Before you share `diagnostic.log`

The diagnostic log is **plain text** and records **previews of message
content** and the **addresses of clusters you connect to**. It never contains
passwords.

If you're attaching it to a bug report or sending it to support, open it
first and remove anything sensitive. It's there to help you diagnose a
problem locally, so treat it as you'd treat the messages themselves.

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
- **Masking is not access control**. See [Section 8](#8-hiding-sensitive-data).
- **New connections default to unencrypted transport**. See
  [Section 2](#2-connecting-to-a-cluster).

### What StreamLens can't do to your cluster

It can't modify ACLs or alter cluster configuration, since none of that is
implemented. What it does do, when you ask it to: read, publish messages,
create and delete topics, and move consumer-group offsets. The last three all
sit behind a typed confirmation on a Production-tagged cluster, and all of
them land in the audit log.

---

## 10. The audit log

**Tools → Audit Log…**

Every write and destructive action taken from this machine is recorded: topics
created and deleted, offsets moved and reset, messages produced, connections
deleted or imported. It's what makes the typed-confirmation guardrail worth
something after the fact, when you need to reconstruct who did what.

The viewer opens with no connection needed, because the trail spans clusters.
Filter it by **cluster**, by **action**, or by free text across the action,
resource, and details. **Export CSV…** writes out whatever you're currently
looking at.

**Events outlive the connections they describe.** Delete a cluster profile and
its actions stay, labelled as deleted rather than disappearing along with the
profile.

It's stored locally in `streamlens.db`. Nothing is uploaded, and the export
goes where you tell it to and nowhere else.

---

## 11. Keeping StreamLens up to date

There's no installer and no auto-updater, so a new version means downloading
a new `StreamLens.exe`. StreamLens tells you when there is one.

### When an update exists

On startup, StreamLens asks GitHub whether a newer version has been released.
If one has, a small notice appears in the bottom-right corner:

> **An update is available.** StreamLens v0.6.0 has been released.
> [Not now] [Download]

**Download** opens your browser and starts downloading the new
`StreamLens.exe` straight away.

**Not now** dismisses that version permanently, so you won't be told about it
again. You will still hear about the version after it.

The notice doesn't block the app, and it doesn't disappear on its own. Leave
it sitting there and deal with it when you're ready.

### Checking on demand

**Help → Check for Updates…** asks immediately rather than waiting for the
daily check, and always answers in the status bar at the bottom: either that
a version is available, that you're on the latest one, or that the check
couldn't be completed.

It also ignores anything you've previously dismissed, so it's the way to find
a version you clicked **Not now** on earlier.

### Installing the new version

Close StreamLens, replace the old `StreamLens.exe` with the new one, and start
it again. That's the whole process.

**Your data carries over.** Connections, settings, per-topic preferences, and
masking rules live in `%LocalAppData%\StreamLensStudio\`, separate from the
executable, and the new version picks them up. You don't need to export
anything first.

Windows shows the SmartScreen warning again for each new file you download,
for the same reason it does on first install.

### What the check actually does

Since this is the one part of StreamLens that talks to the internet, here is
precisely what happens:

- It requests one public GitHub address, the latest release for the
  StreamLens repository. It's the same information you'd see by opening the
  Releases page in a browser, and it needs no login.
- **Nothing about you is sent.** No account, no licence key, no machine name,
  no cluster addresses, no usage data. The request asks what the newest
  version is; that's all of it.
- It runs **at most once a day**, no matter how often you open the app.
- Two things are remembered on your machine, in
  `%LocalAppData%\StreamLensStudio\update-check.json`: when it last checked,
  and which version you dismissed. Deleting that file costs you nothing.
- **Nothing is downloaded or installed on its own.** StreamLens never replaces
  itself or runs anything. **Download** hands a link to your browser, and that
  is where its part ends.

### If you're offline or GitHub is blocked

Nothing happens, and nothing is shown. No error, no dialog, no delay when the
app starts. This is deliberate: a machine with no internet access, a corporate
proxy, or a network that blocks GitHub should be no different from any other,
and a version notice isn't worth interrupting your work over.

The automatic check is silent whether it succeeds or fails. If you want to
know either way, use **Help → Check for Updates…**, which reports the failure
rather than hiding it.

If your network blocks GitHub permanently, the feature simply never does
anything, and there's nothing to switch off.

---

## 12. When something goes wrong

Two things help when the app is being slow, silent, or broken: a live view of
what it's doing, and a way to hand over what happened.

### The Output panel

**Tools → Windows → Output panel** opens a running trace along the bottom of
the window, showing connection steps, topic clicks, and refreshes, each one
timed.

Without it, a slow step looks exactly like a hang. With it open, a connection
that takes twenty seconds says so while it's happening:

> `14:22:07.417  [Connection]  Connect to Prod EU: started`
> `14:22:27.902  [Connection]  Connect to Prod EU: completed in 20485 ms`

Failures show in red and warnings in amber, so one bad step is easy to find in
a long run without reading every line. The controls in the header strip
**collapse** the panel to its title bar, **clear** it, **pin** it open, and
turn off **auto-scroll** for when you want to read back through it without the
newest line pulling you away.

The panel holds the most recent 2,000 lines. None of this is a second log:
every line also goes to `diagnostic.log`, so what you watched on screen and
what a bug report contains will always match.

### If the app crashes

StreamLens catches unhandled errors rather than disappearing mid-click. When
one happens you get a notice in the bottom-right corner telling you what went
wrong, and a self-contained crash report is written to
`%LocalAppData%\StreamLensStudio\crashes\`, named by timestamp.

The notice offers **Show file**, which opens the report, and **Report this
crash**, described below. The 20 most recent reports are kept and older ones
are deleted automatically.

Each report holds the version, your OS, the full exception chain, and the last
200 lines of the diagnostic log. That's usually enough to investigate the
crash without needing anything else from you.

### If a connection fails

A failed connection shows a **Report this…** button next to the error. The
report it prepares isn't the same as a crash report, because nothing has
terminated and so there's no stack trace to capture. What it gets instead is
the connection's settings (security protocol, SASL mechanism, which
certificate paths were set), the error, and the broker's own log lines for
that attempt. Those often name a cause that the app's error message doesn't.

**Passwords are never included.** The report records only whether one was
supplied.

### Filing the report

Both buttons do the same thing, and neither one uploads anything.

1. **A preview opens showing exactly what will be shared.** You can edit it,
   so read it through and delete anything you'd rather not send. These reports
   name clusters, bootstrap servers, and usernames, which for a Kafka tool
   means details about your internal infrastructure.
2. **Copy and open GitHub** puts the full report on your clipboard and opens
   the new-issue form in your browser, with the title and a summary already
   filled in.
3. **You submit it, under your own GitHub account.** Paste the full report
   into the issue body, or attach the file from the `crashes\` folder.

Nothing is sent automatically, and the app has no GitHub credentials of its
own. A report only reaches us if you choose to send it, after seeing what's in
it.

---

## 13. Settings

**Tools → Settings…** covers app-wide defaults: your default format and start
position for topics you haven't opened before, and whether timestamps display
in UTC or local time.

You can keep **multiple settings profiles** and switch between them, which is handy
if you work across teams whose topics use different formats. Mark one as the
default.

### Text size and shortcuts

The whole interface scales with **Ctrl and +** or **Ctrl and −**, and
**Ctrl+0** puts it back to normal. There's a text-size control in the status
bar at the bottom too. Both are worth knowing about on a high-resolution
display, where the default size can be pretty small.

**Ctrl+W** closes the current tab.

---

## 14. Troubleshooting

| What you see | What it usually means |
|---|---|
| **The app won't start** | You're on macOS or Linux. Windows only for now. |
| **The topic list is empty after connecting** | Check the bootstrap address for typos, and that you can reach the cluster from your network. A VPN is often the missing piece. |
| **A topic shows no messages, but you know it has data** | Start position is **Latest**, which only shows recent activity. Switch to **Earliest** to replay from the beginning. |
| **Messages look like unreadable symbols** | Wrong **Format**. Try `Json` or `String`. |
| **An export is full of `\u0000` escapes with readable fragments between them** | An Avro or Protobuf payload exported as `String`. Both begin with a zero byte and a 4-byte schema ID. Set **Format** to `Avro` or `Protobuf` and export again ([Section 7](#7-exporting-messages)). |
| **Another Kafka tool reports "fallback serde was used" on a message you published** | The record went onto the topic as plain text, without Avro or Protobuf encoding. StreamLens publishes every message that way, so don't use it to publish to schema-backed topics. The record can't be repaired in place. See the warning in [Section 5](#5-publishing-a-message). |
| **Avro or Protobuf messages won't decode** | Three possibilities: the Schema Registry URL is missing, the registry needs its own username and password ([Section 2](#2-connecting-to-a-cluster)), or the topic isn't actually in that format. The error message tells the last one apart from the others. Protobuf has no schemaless fallback, so an unregistered payload can't be decoded at all. |
| **Connection fails on a secured cluster** | The **Security Protocol** or **SASL Mechanism** doesn't match what the cluster expects. Confirm both with your Kafka admin. |
| **A certificate error, or the broker drops you mid-handshake** | Usually a missing **CA certificate**, or a cluster that wants a client certificate from you as well. Both are in [Section 2](#2-connecting-to-a-cluster). Don't reach for **Skip certificate verification** to make the error go away. It works, and it also throws away the protection you connected over TLS to get. |
| **A field you masked is still visible** | Check the rule is enabled and its topic field either matches or is blank. Test the pattern in the rules dialog. |
| **You can't delete a topic without typing its name** | Working as intended, since the cluster is tagged **Production**. |
| **A partition's Lag reads `—` instead of a number** | The group has never committed to that partition, so there's no offset to measure lag from. |
| **"Apply offsets" is greyed out** | Nothing has been edited yet. It only lights up once a partition's Offset cell differs from what the broker holds. |
| **You can't move a group's offsets at all** | The group still has running consumers. Kafka refuses the move, so the app refuses first and names the consumers to stop ([Section 6](#6-consumer-groups-lag-and-offsets)). |
| **A Kerberos connection fails on Windows** | Windows authenticates through SSPI as your logged-on account, so you need a domain ticket for the broker's service principal. A keytab path does nothing there ([Section 2](#2-connecting-to-a-cluster)). |
| **Connecting seems to hang** | Open **Tools → Windows → Output panel** to see which step it's on and how long it's been going. Without it, a slow step and a stuck one look identical ([Section 12](#12-when-something-goes-wrong)). |
| **The app showed "Something went wrong"** | It caught an error that would otherwise have ended the session. A crash report was saved, and **Report this crash** on the notice will prepare a GitHub issue from it ([Section 12](#12-when-something-goes-wrong)). |
| **The app closed on its own** | A crash report should still be in `%LocalAppData%\StreamLensStudio\crashes\`, named by the time it happened. Attach the newest one to an issue. |
| **"Couldn't check for updates"** | StreamLens can't reach GitHub. There may be no internet connection, a proxy in the way, or GitHub itself may be down. Nothing is wrong with the app, and the rest of it works normally. Try again later, or check the [Releases page](https://github.com/getaxtools/StreamLens/releases/latest) yourself. |
| **You're never told about updates** | The automatic check is deliberately silent when it fails, so a blocked network looks the same as no new version. Use **Help → Check for Updates…**, which says which it is. |
| **An update notice you dismissed won't come back** | **Not now** hides that version for good. **Help → Check for Updates…** ignores that and will find it again. |

**Still stuck?** **Help → Report a Bug…** opens the issue tracker. After a
crash or a failed connection, use the **Report this crash** or **Report
this…** button instead, since it fills the issue in for you from what actually
happened ([Section 12](#12-when-something-goes-wrong)).

Either way, say what you were doing, what you expected, and what happened. If
you attach `diagnostic.log`, read the warning in
[Section 9](#9-your-data-and-security) first.
