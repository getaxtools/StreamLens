# Kerberos (GSSAPI) stack

A throwaway KDC and a broker that accepts only `SASL_PLAINTEXT/GSSAPI`, for exercising the
app's Kerberos path. Everything in here (the realm, the principals, the passwords in
`setup-kdc.sh`) exists inside these containers and is rebuilt on every `up`.

```bash
cd docker
docker compose -f docker-compose.kerberos.yml up -d
```

Ports: **9095** broker (SASL_PLAINTEXT/GSSAPI), **8088** KDC (udp+tcp), **8089** kadmin.
Independent of the other two stacks; all three can run at once. The `seed-krb` service seeds
the usual JSON topics; there is no Schema Registry in this stack, so the Avro and Protobuf
topics are skipped.

---

## Read this before testing from Windows

librdkafka on Windows does not use MIT Kerberos. It authenticates through native SSPI as
the logged-on Windows user. Two consequences:

- **Keytabs do not work.** Setting `sasl.kerberos.keytab` doesn't degrade gracefully: the
  Confluent client constructor throws `"Kerberos keytabs are not supported in this build"`.
  StreamLens therefore skips that property on Windows (`KafkaClientConfigFactory.KeytabsSupported`),
  so a profile carrying a keytab path stays usable instead of failing to connect.
- **A realistic Windows test needs a real AD domain.** This stack's standalone MIT realm has no
  relationship to your Windows login, so SSPI has no ticket for `kafka/kafka-krb@STREAMLENS.TEST`
  to present.

Use this stack to verify the broker-side and Linux/WSL client paths. Verifying the Windows
SSPI path needs a domain-joined machine and an AD-registered SPN, which a container stack
can't provide.

---

## Verifying the realm works

From inside the network, with the client keytab. This checks that the KDC, the broker's
keytab and the GSSAPI handshake are all correct:

```bash
docker exec streamlens-kafka-krb bash -c '
cat > /tmp/jaas.conf <<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true storeKey=true
    keyTab="/etc/kafka/jaas/client.keytab"
    principal="streamlens@STREAMLENS.TEST";
};
EOF
printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=GSSAPI\nsasl.kerberos.service.name=kafka\n" > /tmp/client.properties
export KAFKA_OPTS="-Djava.security.auth.login.config=/tmp/jaas.conf -Djava.security.krb5.conf=/etc/kafka/jaas/krb5.conf"
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-krb:9095 --command-config /tmp/client.properties'
```

A list of API versions means the handshake succeeded.

---

## Testing from a Linux or WSL client

The `krb-export` service writes what a host-side client needs into `docker/kerberos/out/`
(gitignored):

| File | What it is |
|---|---|
| `client.keytab` | Key for `streamlens@STREAMLENS.TEST` |
| `krb5.host.conf` | Same realm config, but pointing at `localhost` instead of the `kdc` container |

Kerberos binds a service ticket to the hostname the client connected to, so the client must
reach the broker as `kafka-krb`, not `localhost`. Add to `/etc/hosts` (or
`C:\Windows\System32\drivers\etc\hosts`, as Administrator):

```
127.0.0.1 kafka-krb
```

Then:

```bash
export KRB5_CONFIG=$PWD/docker/kerberos/out/krb5.host.conf
kinit -kt docker/kerberos/out/client.keytab streamlens@STREAMLENS.TEST
klist    # should show a ticket for krbtgt/STREAMLENS.TEST
```

In the connection editor:

| Field | Value |
|---|---|
| Bootstrap servers | `kafka-krb:9095` |
| Security protocol | `SASL_PLAINTEXT` |
| SASL mechanism | `GSSAPI` |
| Kerberos service name | `kafka` |
| Kerberos principal | `streamlens@STREAMLENS.TEST` (optional; the ambient ticket is used if blank) |
| Kerberos keytab | `docker/kerberos/out/client.keytab` (ignored on Windows) |

---

## Teardown

```bash
docker compose -f docker-compose.kerberos.yml down -v
```

`-v` matters: the keytabs volume holds keys for a realm that is recreated from scratch next
time, and a stale keytab authenticates against nothing.
