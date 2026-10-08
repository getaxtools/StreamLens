#!/usr/bin/env bash
# Builds a throwaway Kerberos realm for the GSSAPI stack, then runs the KDC in the
# foreground.
#
# Everything here is disposable: the realm, the master key, and every principal are
# recreated from scratch on each `up`. Nothing in this file is a secret worth
# protecting - it exists so GSSAPI has a realm to authenticate against.
#
# Principals created:
#   kafka/kafka-krb@STREAMLENS.TEST   the broker's service principal
#   streamlens@STREAMLENS.TEST        the client the app connects as
#
# Keytabs land in /keytabs, a volume shared with the broker and copied out to
# docker/kerberos/out/ for host-side clients.
set -euo pipefail

REALM="${REALM:-STREAMLENS.TEST}"
KDC_HOST="${KDC_HOST:-kdc}"
BROKER_HOST="${BROKER_HOST:-kafka-krb}"
MASTER_PASSWORD="${MASTER_PASSWORD:-streamlens-master}"
CLIENT_PASSWORD="${CLIENT_PASSWORD:-streamlens-secret}"

KEYTAB_DIR=/keytabs
BROKER_KEYTAB="$KEYTAB_DIR/kafka.keytab"
CLIENT_KEYTAB="$KEYTAB_DIR/client.keytab"

echo "[kdc] building realm $REALM"

# krb5.conf for the containers. The KDC is reachable as `kdc` on the compose network;
# host-side clients get their own copy with localhost addresses (written at the end).
cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = $REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    forwardable = true
    rdns = false
    # Standard MIT defaults. Shorten these to exercise ticket expiry deliberately.
    ticket_lifetime = 24h
    renew_lifetime = 7d

[realms]
    $REALM = {
        kdc = $KDC_HOST:88
        admin_server = $KDC_HOST:749
    }

[domain_realm]
    .streamlens.test = $REALM
    streamlens.test = $REALM
EOF

# kdc.conf - the KDC's own settings, separate from the client-facing krb5.conf.
mkdir -p /etc/krb5kdc
cat > /etc/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    $REALM = {
        database_name = /var/lib/krb5kdc/principal
        admin_keytab = /etc/krb5kdc/kadm5.keytab
        acl_file = /etc/krb5kdc/kadm5.acl
        key_stash_file = /etc/krb5kdc/stash
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
        # aes256 first: the default in modern MIT and what the Java client prefers.
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
    }
EOF

echo '*/admin@'"$REALM"' *' > /etc/krb5kdc/kadm5.acl

# Recreate the database every run. A stale database paired with freshly issued
# keytabs is the single most confusing failure mode in a dev realm - better to
# rebuild both together every time.
rm -rf /var/lib/krb5kdc/*
mkdir -p /var/lib/krb5kdc

echo "[kdc] creating database"
kdb5_util create -s -P "$MASTER_PASSWORD" -r "$REALM"

echo "[kdc] adding principals"
# -randkey for the service principal: it authenticates by keytab, never by password.
kadmin.local -q "addprinc -randkey kafka/$BROKER_HOST@$REALM"
# The client gets a password too, so `kinit streamlens` works interactively as well
# as via the keytab.
kadmin.local -q "addprinc -pw $CLIENT_PASSWORD streamlens@$REALM"

mkdir -p "$KEYTAB_DIR"
rm -f "$BROKER_KEYTAB" "$CLIENT_KEYTAB"

kadmin.local -q "ktadd -k $BROKER_KEYTAB kafka/$BROKER_HOST@$REALM"
kadmin.local -q "ktadd -k $CLIENT_KEYTAB streamlens@$REALM"

# The broker runs as a non-root user in the Kafka image and has to read its keytab.
chmod 644 "$BROKER_KEYTAB" "$CLIENT_KEYTAB"

# JAAS config for the broker, alongside the keytabs so it lands in the same volume.
cat > "$KEYTAB_DIR/kafka-jaas.conf" <<EOF
KafkaServer {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="$BROKER_KEYTAB"
    principal="kafka/$BROKER_HOST@$REALM";
};
EOF

# krb5.conf for the containers, shared so the broker uses exactly what the KDC built.
cp /etc/krb5.conf "$KEYTAB_DIR/krb5.conf"

# A host-side variant: a client on the host reaches the KDC through the published
# port on localhost, not as `kdc`. Written to the same volume; the compose file
# copies this and client.keytab out to docker/kerberos/out/.
sed "s|kdc = $KDC_HOST:88|kdc = localhost:8088|; s|admin_server = $KDC_HOST:749|admin_server = localhost:8089|" \
    /etc/krb5.conf > "$KEYTAB_DIR/krb5.host.conf"

echo "[kdc] realm $REALM ready"
echo "[kdc]   broker principal: kafka/$BROKER_HOST@$REALM"
echo "[kdc]   client principal: streamlens@$REALM (password: $CLIENT_PASSWORD)"

# The kdc service's healthcheck waits for this file, and the broker waits on that
# healthcheck. It only exists once every principal and keytab above succeeded.
touch "$KEYTAB_DIR/.ready"

echo "[kdc] starting krb5kdc in the foreground"
exec krb5kdc -n
