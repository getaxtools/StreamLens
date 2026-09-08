# Masking & Redaction Rules

**Hide sensitive fields — SSNs, card numbers, emails — before they appear on
your screen or in a file you send someone.**

---

## Read this first: what masking does and doesn't do

Masking changes **what StreamLens Studio shows and exports**. It does not
change anything on the Kafka broker, and it does not stop anyone else from
reading the original data.

| Masking **does** protect you from | Masking does **not** protect you from |
|---|---|
| A customer's SSN being visible while you screen-share a debugging session | Someone running `kafka-console-consumer` against the same topic |
| A card number ending up in a CSV you email to a colleague | A teammate who has cluster credentials of their own |
| Sensitive values sitting in a screenshot pasted into a ticket | Anyone reading the messages directly from the broker |

The raw message still arrives from Kafka and still sits in memory — masking
is applied when the message is decoded for display. **It is a hygiene
control, not a security boundary.** Where you need an actual boundary, use
Kafka ACLs; masking sits alongside those, not instead of them.

This is a deliberate design choice, not a limitation waiting to be fixed:
it's what lets StreamLens run with zero infrastructure — no proxy, no
gateway, nothing to deploy.

---

## Quick start: redact a field in 60 seconds

Say `order-created` carries a customer SSN you don't want on screen.

1. Connect to your cluster.
2. **Tools → Masking Rules…**
3. Click **New rule**.
4. Fill in:
   - **Rule name:** `Redact SSN`
   - **Topic:** leave empty (applies to every topic on this cluster)
   - **Match by:** `JsonPath`
   - **Pattern:** `$.customer.ssn`
   - **Replace with:** `Redact`
5. Click **Save rule**, then **Close**.

Open `order-created`. Where the SSN used to be, you'll now see `***`.

```jsonc
// Before                              // After
{                                      {
  "orderId": "ord_5512",                 "orderId": "ord_5512",
  "customer": {                          "customer": {
    "name": "Ada Lovelace",                "name": "Ada Lovelace",
    "ssn": "123-45-6789"                   "ssn": "***"
  },                                     },
  "total": 42.50                         "total": 42.50
}                                      }
```

Everything else is untouched, and the payload is still valid JSON — so
pretty-printing and the detail pane keep working normally.

---

## Where masking applies

Once a rule is on, it covers **every** place that message text appears:

- The message grid
- The detail pane (including its JSONPath search)
- Row search / filtering
- **Find Messages** results
- CSV, JSON, and per-message ZIP exports

You don't enable it per surface — there's nothing to forget. If a rule is
on, the masked value is the only version the app will show you or write to
a file.

> **One consequence worth knowing:** because search runs on masked text, you
> can't find a message in the grid by typing a value a rule hides. That's
> intended — a searchable "redacted" field isn't redacted. **Find Messages**
> behaves differently and deliberately so: see
> [Find Messages](#find-messages-searches-the-real-value) below.

---

## Choosing how to match

### JSONPath — when you know the field

Best when your payload has a stable shape and you know exactly which field
is sensitive.

| Pattern | Matches |
|---|---|
| `$.ssn` | Top-level `ssn` |
| `$.customer.ssn` | Nested `ssn` under `customer` |
| `$.items[*].cardNumber` | `cardNumber` in **every** element of `items` |
| `$..ssn` | `ssn` **anywhere**, at any depth |
| `$.customer` | The **whole** `customer` object |
| `$['order.id']` | A field whose name contains a dot |

Two worth calling out:

- **`$..ssn` (recursive descent)** is the one to reach for when you're not
  certain everywhere a field appears. It catches `$.ssn`, `$.customer.ssn`,
  and `$.audit.trail[0].ssn` in one rule.
- **Naming an object** (`$.customer`) masks the entire subtree, not just its
  scalar fields. Use it when the whole structure is sensitive.

Filter expressions (`$.items[?(@.price > 100)]`) are **not** supported. If
you enter one, the rule is rejected when you save it with a message saying
so — it won't be saved as a rule that silently does nothing.

### Regex — when you don't

Best when the payload shape varies, isn't JSON at all, or the sensitive
value could appear anywhere in free text.

| Pattern | Use |
|---|---|
| `\d{3}-\d{2}-\d{4}` | US SSN format, wherever it appears |
| `[\w.+-]+@[\w-]+\.[\w.]+` | Email addresses |
| `\b\d{13,16}\b` | Card-length digit runs |
| `account=(\d+)` | See "capturing groups" below |

**Capturing groups narrow what gets masked.** With no group, the whole match
is replaced:

```
account=123456 status=ok    →    *** status=ok
```

With a group around just the sensitive part, the label survives:

```
Pattern: account=(\d+)
account=123456 status=ok    →    account=*** status=ok
```

That second form is almost always what you want — it keeps the payload
readable while still hiding the value.

Regex rules apply to **any** format, including JSON. If your JSON shape is
inconsistent, a regex rule is often simpler than several JSONPath rules.

---

## Choosing what to replace it with

### Redact — `***`

Total replacement. Use for anything you never need to see: SSNs, passwords,
full card numbers, government IDs.

### Show last N — `************1111`

Keeps a trailing few characters. Use when the tail is how a human tells two
records apart — card numbers, account IDs, phone numbers.

Set **Trailing characters left visible** (default `4`).

> **Short values fall back to full redaction automatically.** If the value
> isn't long enough for the mask to mean anything — a 4-character PIN with
> 4 characters visible — you get `***` instead. Showing 4 of 4 characters
> isn't a mask, so the app won't pretend it is.

### Tokenize — `tok_a3f91e04`

Replaces the value with a short, stable token. The same input always
produces the same token, so **you can still correlate**:

```jsonc
// Message 1                          // Message 2
{ "customerId": "tok_a3f91e04",       { "customerId": "tok_a3f91e04",
  "event": "cart.add" }                 "event": "cart.checkout" }
```

You can see both events belong to the same customer without learning who
that customer is. This is the option that keeps debugging possible under
redaction.

> **Tokens are not cryptographically secure.** They're a truncated hash of
> the original value with no salt — deliberately, since a salt would break
> the correlation that makes the feature useful. Someone who can guess
> candidate values can confirm a match by hashing them. For anything where
> that matters, use **Redact**.

---

## Scoping rules

### Per topic vs. cluster-wide

Leave **Topic** empty and the rule applies to every topic on the cluster.
Fill it in and it applies only to that one.

A good default: **cluster-wide for formats, per-topic for fields.**

- An email regex → cluster-wide. Emails are sensitive wherever they appear.
- `$.customer.ssn` → per-topic. That path only means something on topics
  with that shape.

Cluster-wide rules sort to the top of the rules list, since they're the
broader context the per-topic ones sit inside.

Rules are stored **per cluster**. A rule on your Production connection does
not exist on Staging — worth knowing, because it means connecting to a new
production cluster starts with no masking until you set it up.

### Keys

By default, rules apply to the message **value** only. Message keys are
usually the identifier you navigate by (an order ID, a customer reference),
so masking them by default would make the grid hard to use.

Tick **Also apply to the message key** on rules where the key itself is
sensitive.

---

## Managing rules

Each rule in the list shows its name, pattern, scope, match type, strategy,
and target, with three actions:

| Action | Effect |
|---|---|
| **Disable** / **Enable** | Turns the rule off/on without deleting it. A disabled rule is labelled `(disabled)` — it masks nothing. |
| **Edit** | Opens the rule for changes. Cancelling discards them. |
| **Delete** | Removes the rule permanently. |

Changes take effect on the **next batch of messages**. Already-visible rows
keep their current text until they scroll out and are replaced, or until you
switch value format (which re-decodes and re-masks everything on screen).

### Every change is logged

Creating, editing, deleting, enabling, and disabling a rule are all written
to the local audit log — with **disabling logged distinctly from enabling**,
since "a redaction rule was switched off at 14:02" is exactly what a
compliance review needs to find.

The log records the rule (name, pattern, scope, strategy) and **never a
sample of what it matched** — putting masked data in the audit log would
defeat the point.

---

## Behaviour worth knowing about

### Broken rules are reported, never silently ignored

If a pattern won't compile, the app tells you when you try to save it and
refuses to save. A rule that appears active in the list but masks nothing is
the worst possible outcome — you'd believe a field was hidden while it was
on screen — so the app won't let one exist.

If a saved rule somehow fails to compile later, the rules dialog shows the
problem in red at the top of the list.

### A slow regex withholds the value rather than showing it

Some regex patterns backtrack catastrophically on certain inputs. If a rule
takes longer than 100ms on one message, that message's value is replaced
with:

```
[masking rule 'Your rule name' timed out — value withheld]
```

The app **fails closed**: rather than showing you unmasked data, it shows
you nothing and names the rule at fault. If you see this, simplify the
pattern — nested quantifiers like `(a+)+` are the usual cause.

### Switching value format re-masks

Changing **Value format** (`Raw` / `String` / `Json` / `Avro` / `Hex`)
re-decodes from the original bytes — and re-applies masking. You can't
reveal a masked field by switching format.

### Find Messages searches the real value

**Find Messages** (broker-side scan) matches against the **unmasked** text,
then masks the results before displaying them.

This is deliberate. You typed the search term yourself, so matching on the
real value tells you nothing you didn't already supply — while masking
first would make a message unfindable by a term you know. You can find the
message; you just can't read the masked field in the result.

---

## Worked examples

### Card numbers, last four visible

```
Rule name:  Card — last 4
Topic:      (empty — all topics)
Match by:   Regex
Pattern:    \b(\d{13,16})\b
Replace:    Show last 4
Trailing:   4
```
`4111111111111111` → `************1111`

### Every email, everywhere

```
Rule name:  Emails
Topic:      (empty)
Match by:   Regex
Pattern:    [\w.+-]+@[\w-]+\.[\w.]+
Replace:    Redact
```

### Customer IDs, still correlatable

```
Rule name:  Customer ID
Topic:      (empty)
Match by:   JsonPath
Pattern:    $..customerId
Replace:    Tokenize
Also apply to key: ✔
```
Ticking the key box matters here — if `customerId` is also the partition
key, leaving it unticked would leave it in plain view in the Key column.

### An entire PII block on one topic

```
Rule name:  PII block
Topic:      user-events
Match by:   JsonPath
Pattern:    $.pii
Replace:    Redact
```

---

## Before a screen-share: a checklist

1. **Tools → Masking Rules…** — confirm your rules are listed and none say
   `(disabled)`.
2. Check the **scope** column — a rule scoped to `order-created` won't
   protect `order-created-v2`.
3. Open the topic and confirm with your own eyes that the field is masked.
4. Remember masking is **per cluster** — rules on Staging don't exist on
   Production.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| Rule saved but nothing is masked | Check scope (topic name must match exactly) and that the rule isn't `(disabled)`. Rules also apply per cluster. |
| JSONPath rule does nothing | The payload may not be JSON, or the path doesn't match. Confirm the shape in the detail pane, then test the same path in detail-pane search — if it finds nothing there, the path is wrong. |
| Masking applied but the field is still visible elsewhere | It's likely a *different* field with a similar name, or the same value duplicated at another path. Try `$..fieldName` to catch every depth. |
| `[masking rule '…' timed out]` in place of a value | A regex is backtracking badly. Simplify it — avoid nested quantifiers. |
| Can't find a message by searching for a value you know | Expected: grid search runs on masked text. Use **Find Messages**, which scans the real values. |
| Rules vanished after switching clusters | Rules are per cluster by design. |
| A rule won't save | Read the message under the form — an unparseable pattern is refused rather than saved as a rule that does nothing. |

---

## Related

- [Usage Guide index](usageguide.md)
- [Publishing messages](publishing-messages.md) — pre-flight schema validation
- Product spec: `../streamlens-studio-spec.md` §3.6
- Security posture: `../security-audit.md`
