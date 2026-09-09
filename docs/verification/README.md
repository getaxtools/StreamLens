# Verifying your download

`StreamLens.exe` is signed with a **self-signed** certificate. This page explains what that
does and doesn't tell you, and how to check a download.

## Read this first: the signature does not prove authenticity

The certificate below signs itself. It is not issued by a certificate authority, so nothing
about it has been verified by a third party — anyone can generate a certificate claiming any
name, including this one.

Concretely, that means:

- Windows still shows a SmartScreen warning on first run. The signature does not remove it.
- `Get-AuthenticodeSignature` reports `UnknownError`, not `Valid`, with the message
  *"terminated in a root certificate which is not trusted by the trust provider"*.
  That is expected here, not a sign of tampering.
- The signature alone **cannot** tell you a download is genuine.

The way to confirm a download is genuine is the SHA-256 hash published with each release
(see [Checking the hash](#checking-the-hash) below). That check is meaningful; the signature
check is not.

A CA-issued certificate is planned. When one is in place, signature validation will start
returning `Valid` and this page will be updated.

## Do not install this certificate

Do **not** add this certificate to your Trusted Root Certification Authorities store.

Doing so would make your machine trust every program its private key ever signs, indefinitely.
That key lives in a single developer's Windows certificate store. Installing it buys you
nothing — the download is verified by its hash, not by this certificate — while permanently
weakening your machine's trust settings.

Treat any project that asks you to install its root certificate with suspicion. Including
this one.

## Certificate details

| Field | Value |
|---|---|
| Subject | `CN=StreamLens, O=StreamLens Dev` |
| SHA-256 (of the `.cer` file) | `7ABEADF1D832E0150785BF65F55823C2BE60D574A7447E9EB7618D7190DFFD01` |
| SHA-1 thumbprint | `07E289C39A6BDEA660521AC8A77873F6561A7CC6` |
| Valid until | 2031-09-08 |
| Key usage | Digital Signature, Code Signing |

The file [`StreamLensPublic.cer`](StreamLensPublic.cer) contains only the public certificate.
It holds no private key.

## Checking the hash

This is the check that actually establishes the file you downloaded is the file that was
published. Compare the output against the SHA-256 value in the
[release notes](https://github.com/getaxtools/StreamLens/releases):

```powershell
Get-FileHash .\StreamLens.exe -Algorithm SHA256
```

If the hash matches, the download is intact and unmodified. If it doesn't, delete the file
and download it again from the
[Releases page](https://github.com/getaxtools/StreamLens/releases) — do not run it.

## Inspecting the signature (optional)

To see the embedded signature without installing anything:

```powershell
Get-AuthenticodeSignature .\StreamLens.exe | Format-List
```

Expect `Status: UnknownError` and a signer of `CN=StreamLens, O=StreamLens Dev`, for the
reasons described above. The timestamp is issued by a real timestamp authority (DigiCert), so
`TimeStamperCertificate` will show a genuine CA-issued certificate — that confirms *when* the
file was signed, not *who* signed it.

## Questions

Open an [issue](https://github.com/getaxtools/StreamLens/issues).
