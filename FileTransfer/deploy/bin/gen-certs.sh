#!/usr/bin/env bash
# gen-certs.sh — generate a self-signed CA and a server cert/key for the master.
#
#   certs/server.key      server private key
#   certs/server.crt      server public cert (signed by the CA below)
#   certs/CAs/ca.crt      CA cert — the workers' trust store
#   certs/CAs/ca.key      CA private key (keep safe; only needed to sign more certs)
#
# Re-run to rotate. Add extra SANs by editing the SAN list below.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # Home dir: bin certs config lib logs tmp
export FT_HOME="$ROOT_DIR"
CERT_DIR="$ROOT_DIR/certs"
CA_DIR="$CERT_DIR/CAs"
DAYS="${CERT_DAYS:-825}"
# Subject Alternative Names the server cert is valid for (edit for your hosts).
SANS="${CERT_SANS:-DNS:localhost,DNS:filetransfer-master,IP:127.0.0.1}"

mkdir -p "$CA_DIR"

echo "==> Certificate Authority (trust store)"
if [ ! -f "$CA_DIR/ca.key" ]; then
    openssl genrsa -out "$CA_DIR/ca.key" 4096
    openssl req -x509 -new -nodes -key "$CA_DIR/ca.key" -sha256 -days 3650 \
        -subj "/O=FileTransfer/CN=FileTransfer Root CA" \
        -out "$CA_DIR/ca.crt"
    echo "    created $CA_DIR/ca.crt"
else
    echo "    reusing existing $CA_DIR/ca.crt"
fi

echo "==> Server key + cert (SANs: $SANS)"
openssl genrsa -out "$CERT_DIR/server.key" 2048
openssl req -new -key "$CERT_DIR/server.key" \
    -subj "/O=FileTransfer/CN=filetransfer-master" \
    -out "$CERT_DIR/server.csr"

cat > "$CERT_DIR/server.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = $SANS
EOF

openssl x509 -req -in "$CERT_DIR/server.csr" \
    -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/server.crt" -days "$DAYS" -sha256 \
    -extfile "$CERT_DIR/server.ext"

rm -f "$CERT_DIR/server.csr" "$CERT_DIR/server.ext"
chmod 600 "$CERT_DIR/server.key" "$CA_DIR/ca.key"

# Worker client cert (for mTLS: the worker presents this to the master).
echo "==> Worker client key + cert (CN=filetransfer-worker)"
openssl genrsa -out "$CERT_DIR/client.key" 2048
openssl req -new -key "$CERT_DIR/client.key" -subj "/O=FileTransfer/CN=filetransfer-worker" -out "$CERT_DIR/client.csr"
cat > "$CERT_DIR/client.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in "$CERT_DIR/client.csr" -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/client.crt" -days "$DAYS" -sha256 -extfile "$CERT_DIR/client.ext"
rm -f "$CERT_DIR/client.csr" "$CERT_DIR/client.ext"
chmod 600 "$CERT_DIR/client.key"

echo "==> Done"
echo "    server cert : $CERT_DIR/server.crt"
echo "    server key  : $CERT_DIR/server.key"
echo "    worker cert : $CERT_DIR/client.crt (CN=filetransfer-worker)"
echo "    trust store : $CA_DIR/ca.crt"
echo
echo "    For an application flow, mint a client cert whose CN matches the flow's cn:"
echo "        bin/gen-client-cert.sh <CN>   # e.g. acme-billing.client"
