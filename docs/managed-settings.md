# Manage StreamLens Studio settings for your organization

You can enforce some StreamLens Studio settings on the computers you manage: on Windows with
Group Policy or Intune, on macOS with a configuration profile, and on Linux with a policy file.
StreamLens Studio reads these settings when it starts, and users can't change them. Managed
settings are available from v0.9.0.

> **Note:** Managed settings are a client-side safeguard, not access control. A user can still
> connect with another Kafka client. To control who can write to a cluster, use Kafka ACLs and
> Schema Registry authorization.

## Available settings

| Setting | Value name | Effect when on |
|---|---|---|
| Turn off update checks | `DisableUpdateCheck` | StreamLens Studio doesn't contact GitHub for new releases, and **Help ▸ Check for Updates** is hidden. |
| Turn off reporting issues to GitHub | `DisableIssueReporting` | The **Report this crash** button and the **Report this…** button shown after a failed connection are hidden. These reports can include cluster host names and user names. Crash reports are still saved locally. |
| Require TLS certificate verification | `DisallowSkipTlsVerification` | **Skip certificate verification** is turned off in the connection editor. Saved connections that have it selected connect with verification on. |
| Don't save connection passwords | `DisallowSavedPasswords` | SASL, client key and Schema Registry passwords aren't saved to the OS credential store. Users enter them once per session. StreamLens Studio asks for a missing SASL or Schema Registry password when they connect; a client key password must be entered again in the connection editor. Passwords saved earlier aren't used. |
| Make connections read-only | `ReadOnlyLocal`, `ReadOnlyDevelopment`, `ReadOnlyStaging`, `ReadOnlyProduction` | Connections in that environment can't publish or republish messages, create or delete topics, or move or reset consumer group offsets. Browsing is unaffected. |

Each value is on when it's `1` (or `true`), and off when it's `0`, `false` or not set.

The environment is a label that users choose for each connection. To make every connection
read-only regardless of its label, turn on all four `ReadOnly` values.

To see which settings apply on a computer, open **Help ▸ About StreamLens Studio**. Active
settings are listed under **MANAGED BY YOUR ORGANIZATION**.

## Windows

StreamLens Studio reads these registry keys, in this order:

1. `HKEY_LOCAL_MACHINE\SOFTWARE\Policies\StreamLens`
2. `HKEY_CURRENT_USER\SOFTWARE\Policies\StreamLens`

If a value is set in both, the computer setting applies. Values are `REG_DWORD`.

### Use Group Policy

1. Get `StreamLens.admx` and `en-US/StreamLens.adml` from the
   [`policy`](../policy/) folder of this repository.
2. Copy `StreamLens.admx` to `%SystemRoot%\PolicyDefinitions`, and `en-US\StreamLens.adml` to
   `%SystemRoot%\PolicyDefinitions\en-US`. For a domain, copy them to the
   [central store](https://learn.microsoft.com/troubleshoot/windows-client/group-policy/create-and-manage-central-store)
   instead.
3. Open the Group Policy Management Editor, or `gpedit.msc` for a single computer.
4. Go to **Computer Configuration** (or **User Configuration**) **▸ Administrative Templates ▸
   StreamLens Studio**, and configure the settings.

### Use Intune

Import the ADMX file as described in
[Import custom ADMX files](https://learn.microsoft.com/mem/intune/configuration/administrative-templates-import-custom),
then create a configuration profile from the **StreamLens Studio** templates.

### Set the values directly

To try a setting on one computer, run this in an elevated command prompt:

```cmd
reg add HKLM\SOFTWARE\Policies\StreamLens /v ReadOnlyProduction /t REG_DWORD /d 1 /f
```

## macOS

> **Note:** There's no macOS build yet. These settings take effect once it ships; see the
> [roadmap](../README.md#roadmap).

Deploy a configuration profile with a custom settings payload for the preference domain
`io.github.getaxtools.StreamLens`. Use the value names from the table as keys, with Boolean
values:

```xml
<key>DisableUpdateCheck</key>
<true/>
<key>ReadOnlyProduction</key>
<true/>
```

Your MDM writes the profile to `/Library/Managed Preferences`. If a value is set at both the
computer and user level, the computer setting applies.

## Linux

> **Note:** There's no Linux build yet. These settings take effect once it ships; see the
> [roadmap](../README.md#roadmap).

Create `/etc/streamlens/policies.json`, owned by root and not writable by other users:

```json
{
  "DisableUpdateCheck": true,
  "DisallowSavedPasswords": true,
  "ReadOnlyProduction": true
}
```

## Troubleshooting

If a setting doesn't apply, restart StreamLens Studio; it reads settings only at startup. Then
check `diagnostic.log` in the StreamLens Studio data folder. Each active setting is logged with
a `POLICY` prefix, and a policy source that can't be read, such as malformed JSON, is logged
with the reason. StreamLens Studio skips a source it can't read and keeps running.
