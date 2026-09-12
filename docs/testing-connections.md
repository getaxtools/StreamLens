# Testing connection types against the dev stacks

How to drive StreamLens through each connection type the app supports, using the
local Docker stacks in [`docker/`](../docker). This is the app-side walkthrough -
which fields to fill in the connection dialog. For what the stacks themselves
start, which ports they expose, and what data gets seeded, see
[`docker/README.md`](../docker/README.md).

See also the [usage guide](usage_guide.md) for using the app once connected.

## Prerequisites

Both stacks can run at the same time - different project names, containers, ports
and volumes.

```bash
cd docker
docker compose up -d                              # plaintext: 9092, registry 8081
docker compose -f docker-compose.tls.yml up -d    # TLS: 9093/9094, registry 8082
```

Export the TLS certificates somewhere the app can read them:

```bash
mkdir -p "$HOME/streamlens-certs"
docker run --rm -v streamlens-tls_certs:/certs -v "$HOME/streamlens-certs:/out" \
  alpine sh -c "cp /certs/ca.pem /certs/client.pem /certs/client.key /out/"
```

Paths below use `C:\Users\<you>\streamlens-certs\`. Substitute your own.

### Check the Avro topic seeded

The seeder can race Schema Registry on a cold start: the registry takes ~30s to
accept requests, and the plaintext stack's `seed` only waits for the container to
*start*, not to be ready. When it loses that race it logs
`WARNING: Avro seeding skipped` and `sensor-readings-avro` ends up with no
registered schema. Verify:

```bash
curl -s http://localhost:8081/subjects                                    # plaintext
curl -s -k -u registry:registry-secret https://localhost:8082/subjects    # TLS
```

Each should list `sensor-readings-avro-value`. If either returns `[]`, re-run the
seeder now that the registry is up:

```bash
docker compose up seed --build --force-recreate
```

(The TLS stack's `seed-tls` waits on a real registry healthcheck, so it does not
hit this.)

## The connection dialog

One dialog for every connection type. Open it from **Add Connection…** in the
menu, or the **+ Add connection** button.

Always visible:

1. **Name** - anything, e.g. `Local Plaintext`
2. **Bootstrap Servers** - per connection below
3. **Environment** - `Development` or `Local`. Avoid `Production`: it switches on
   guardrail theming and an extra confirmation on every action against the profile.
4. **Tab Color** - optional. Useful when testing all four, since the color
   cascades to that connection's topic tabs.

Everything else is behind **Advanced options**. The TLS file fields only appear
once **Security Protocol** is `Ssl` or `SaslSsl` - they stay hidden on plaintext,
where librdkafka would ignore them.

**Test Connection** sits bottom-left, separate from **Cancel** / **Add**. Test
before saving.

## 1. Plaintext - `localhost:9092`

- **Bootstrap Servers:** `localhost:9092`
- Leave **Advanced options** collapsed. Nothing else is required.

For Avro decoding, expand Advanced and set **Schema Registry URL** to
`http://localhost:8081`. No credentials - this registry is unauthenticated.

## 2. Mutual TLS - `localhost:9093`

- **Bootstrap Servers:** `localhost:9093`
- **Advanced options** → expand
- **Security Protocol:** `Ssl`  *(TLS fields appear)*
- **SASL Mechanism:** `None`; leave SASL username/password blank
- **CA Certificate Path:** `C:\Users\<you>\streamlens-certs\ca.pem`
- **Client Certificate Path:** `C:\Users\<you>\streamlens-certs\client.pem`
- **Client Key Path:** `C:\Users\<you>\streamlens-certs\client.key`
- **Client Key Password:** blank
- **Skip certificate verification:** unchecked

The broker sets `ssl.client.auth=required` on this port, so this is the one case
where omitting the client certificate and key is a hard handshake rejection rather
than a degraded connection. Worth running once with those two fields blank to
exercise the failure path.

## 3. SASL_SSL + SCRAM - `localhost:9094`

- **Bootstrap Servers:** `localhost:9094`
- **Advanced options** → expand
- **Security Protocol:** `SaslSsl`
- **SASL Mechanism:** `ScramSha512`
- **SASL Username:** `streamlens`
- **SASL Password:** `streamlens-secret`
- **CA Certificate Path:** `C:\Users\<you>\streamlens-certs\ca.pem`
- **Client Certificate Path / Client Key Path:** **blank**

This port authenticates by password, not certificate. The client cert fields are
visible because the transport is TLS, which makes filling them in the common
mix-up here.

## 4. Schema Registry over HTTPS + basic auth

Not a separate connection - add it to profile 2 or 3. Below the TLS block in the
same Advanced section:

- **Schema Registry URL:** `https://localhost:8082`
- **Schema Registry Username:** `registry`
- **Schema Registry Password:** `registry-secret`

These are deliberately *not* the SASL fields above them. The registry is a separate
service with its own credentials - the shape a Confluent Cloud registry has, where
the broker login is SASL but the registry is behind basic auth. Crossing the two
sets of credentials is precisely what this field pair exists to surface.

## Protocols with no local coverage

`SecurityProtocolKind` also offers `SaslPlaintext`, and `SaslMechanismKind` offers
`Plain`, `ScramSha256`, `Gssapi` and `OAuthBearer`. Neither compose stack starts a
listener for these, so there is no local broker to test them against - they can be
configured and saved, but not connected.

## Notes

- SASL passwords and client key passwords go to the OS credential store, not the
  profile. On **Edit** these fields load blank by design - `ClusterProfile` never
  holds secrets. Not data loss; re-enter only when changing them.
- A CA path is required on all three TLS connections. These certificates are signed
  by a throwaway private CA that the Windows trust store knows nothing about -
  which is the point, since a public-CA cluster never exercises that path.
  **Skip certificate verification** works as an escape hatch but defeats the test.
- `docker compose -f docker-compose.tls.yml down -v` discards the generated
  certificates. The next start issues new ones, and all three TLS profiles need
  their certificate files re-exported (same paths, new contents).
