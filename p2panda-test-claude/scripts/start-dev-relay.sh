#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${IROH_RELAY_STATE_DIR:-$ROOT_DIR/.local/iroh-relay}"
CERT_DIR="$STATE_DIR/certs"
CONFIG_PATH="$STATE_DIR/config.toml"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/p2panda-build/iroh-relay}"

RELAY_SRC="${IROH_RELAY_SRC:-}"
if [[ -z "$RELAY_SRC" ]]; then
  RELAY_SRC="$(find "$HOME/.cargo/registry/src" -maxdepth 2 -type d -name 'iroh-relay-*' | sort | tail -n 1)"
fi

if [[ -z "$RELAY_SRC" || ! -d "$RELAY_SRC" ]]; then
  echo "Could not locate cached iroh-relay source under ~/.cargo/registry/src" >&2
  echo "Build a crate that depends on iroh-relay first, or set IROH_RELAY_SRC explicitly." >&2
  exit 1
fi

mkdir -p "$CERT_DIR"

if [[ ! -f "$CERT_DIR/cert.pem" || ! -f "$CERT_DIR/cert.key.pem" ]]; then
  openssl req \
    -x509 \
    -newkey rsa:2048 \
    -sha256 \
    -nodes \
    -days 365 \
    -keyout "$CERT_DIR/cert.key.pem" \
    -out "$CERT_DIR/cert.pem" \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1"
fi

mkdir -p "$STATE_DIR"
cat > "$CONFIG_PATH" <<EOF
enable_quic_addr_discovery = true

[tls]
cert_mode = "Manual"
manual_cert_path = "$CERT_DIR/cert.pem"
manual_key_path = "$CERT_DIR/cert.key.pem"
EOF

echo "Relay config: $CONFIG_PATH"
echo "Relay URL: http://localhost:3340"
echo "QUIC discovery port: 7824"
echo "App flags: --relay-url http://localhost:3340 --insecure-skip-relay-cert-verify"

CARGO_TARGET_DIR="$TARGET_DIR" cargo run \
  --offline \
  --manifest-path "$RELAY_SRC/Cargo.toml" \
  --features server \
  --bin iroh-relay \
  -- \
  --config-path "$CONFIG_PATH" \
  --dev
