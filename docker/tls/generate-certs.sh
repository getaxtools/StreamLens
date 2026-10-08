#!/usr/bin/env bash
# Generates a throwaway PKI for the TLS dev stack: a private CA, a server certificate for the
# broker, and a client certificate/key for testing mutual TLS from StreamLens.
#
# The point of a *private* CA here is that it is exactly the case the app could not handle
# before: nothing in the Windows trust store signs these, so connecting requires pointing the
# app at ca.pem explicitly. Everything is regenerated only if missing, so restarting the stack
# keeps the same certificates and a saved connection profile stays valid.
set -euo pipefail

CERT_DIR=${CERT_DIR:-/certs}
DAYS=3650
STORE_PASS=${STORE_PASS:-streamlens}

cd "$CERT_DIR"

if [ -f ca.pem ] && [ -f kafka.keystore.jks ] && [ -f client.pem ]; then
  echo "Certificates already present in $CERT_DIR - nothing to do."
  exit 0
fi

echo "Generating throwaway CA and certificates in $CERT_DIR…"

# 1. The CA that signs everything below.
openssl req -new -x509 -keyout ca.key -out ca.pem -days "$DAYS" -nodes \
  -subj "/CN=streamlens-dev-ca/O=StreamLens/C=US" 2>/dev/null

# 2. Server certificate, shared by the broker and the Schema Registry. The SAN list is what a
#    client's hostname check matches against, so it has to cover every name either service is
#    reached by: localhost (the app on the host), kafka-tls and schema-registry-tls (inside the
#    compose network).
openssl req -new -keyout kafka.key -out kafka.csr -nodes \
  -subj "/CN=localhost/O=StreamLens/C=US" 2>/dev/null

cat > kafka-ext.cnf <<EOF
subjectAltName = DNS:localhost, DNS:kafka-tls, DNS:schema-registry-tls, IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF

openssl x509 -req -in kafka.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out kafka.pem -days "$DAYS" -extfile kafka-ext.cnf 2>/dev/null

# 3. Client certificate for mutual TLS. clientAuth is what lets the broker accept it as an
#    identity rather than just another certificate.
openssl req -new -keyout client.key -out client.csr -nodes \
  -subj "/CN=streamlens-client/O=StreamLens/C=US" 2>/dev/null

cat > client-ext.cnf <<EOF
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.pem -days "$DAYS" -extfile client-ext.cnf 2>/dev/null

# 4. Kafka itself speaks JKS, so the broker's certificate and the CA it trusts get packed into
#    a keystore/truststore pair. The client side stays as PEM files, which is what librdkafka
#    (and so StreamLens) reads.
openssl pkcs12 -export -in kafka.pem -inkey kafka.key -chain -CAfile ca.pem \
  -name kafka -out kafka.p12 -password "pass:$STORE_PASS" 2>/dev/null

keytool -importkeystore -noprompt \
  -deststorepass "$STORE_PASS" -destkeypass "$STORE_PASS" -destkeystore kafka.keystore.jks \
  -srckeystore kafka.p12 -srcstoretype PKCS12 -srcstorepass "$STORE_PASS" \
  -alias kafka 2>/dev/null

keytool -import -noprompt -alias ca -file ca.pem \
  -keystore kafka.truststore.jks -storepass "$STORE_PASS" 2>/dev/null

echo "$STORE_PASS" > keystore-credentials.txt

# The app reads these directly from the host, so they must not be root-only.
chmod 644 ./*.pem ./*.key ./*.jks keystore-credentials.txt

rm -f ./*.csr ./*-ext.cnf kafka.p12

echo "Done. CA: $CERT_DIR/ca.pem  client cert: client.pem / client.key"
