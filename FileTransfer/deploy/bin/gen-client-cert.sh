#!/usr/bin/env bash
# gen-client-cert.sh — mint a client certificate (signed by our CA) for an application
# flow. The certificate's CN must match the `cn:` in the flow's YAML so the master can
# authenticate and authorize the app under mTLS.
#
#   bin/gen-client-cert.sh acme-billing.client
#   -> certs/clients/acme-billing.client.crt  +  .key
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CERT_DIR="$ROOT_DIR/certs"
CA_DIR="$CERT_DIR/CAs"
OUT_DIR="$CERT_DIR/clients"
DAYS="${CERT_DAYS:-825}"

CN="${1:-}"
if [ -z "$CN" ]; then
    echo "usage: gen-client-cert.sh <CN>   (CN must match a flow's cn:)"
    exit 2
fi
if [ ! -f "$CA_DIR/ca.key" ]; then
    echo "ERROR: CA not found — run bin/gen-certs.sh first."
    exit 1
fi

mkdir -p "$OUT_DIR"
KEY="$OUT_DIR/$CN.key"; CRT="$OUT_DIR/$CN.crt"; CSR="$OUT_DIR/$CN.csr"; EXT="$OUT_DIR/$CN.ext"

openssl genrsa -out "$KEY" 2048
openssl req -new -key "$KEY" -subj "/O=FileTransfer/CN=$CN" -out "$CSR"
cat > "$EXT" <<EOF
basicConstraints=CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in "$CSR" -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" -CAcreateserial \
    -out "$CRT" -days "$DAYS" -sha256 -extfile "$EXT"
rm -f "$CSR" "$EXT"
chmod 600 "$KEY"

echo "Client certificate for CN=$CN:"
echo "  cert : $CRT"
echo "  key  : $KEY"
echo "  (the app presents these; the master trusts them via certs/CAs/ca.crt)"
