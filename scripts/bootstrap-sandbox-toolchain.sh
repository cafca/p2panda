#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/bootstrap-sandbox-toolchain.sh [--provider claude|codex] [--sandbox name] [--template image] [--skip-fetch]

Prepare a plain Docker sandbox container for Rust verification.

Options:
  -p, --provider  Sandbox flavor. Defaults to codex.
      --sandbox   Override the container name. Defaults to <provider>-p2panda.
      --template  Override the image. Defaults to docker/sandbox-templates:<provider>.
      --skip-fetch Skip `cargo fetch --locked` after package installation.
  -h, --help      Show this help text.
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROVIDER="codex"
SANDBOX=""
TEMPLATE=""
SKIP_FETCH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--provider)
      PROVIDER="$2"
      shift 2
      ;;
    --sandbox)
      SANDBOX="$2"
      shift 2
      ;;
    --template)
      TEMPLATE="$2"
      shift 2
      ;;
    --skip-fetch)
      SKIP_FETCH=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

case "$PROVIDER" in
  claude|codex) ;;
  *)
    echo "Unsupported provider: $PROVIDER" >&2
    exit 1
    ;;
esac

SANDBOX="${SANDBOX:-${PROVIDER}-p2panda}"
TEMPLATE="${TEMPLATE:-docker/sandbox-templates:${PROVIDER}}"

ensure_sandbox() {
  if docker ps --format '{{.Names}}' | grep -Fxq "$SANDBOX"; then
    return 0
  fi

  if docker ps -a --format '{{.Names}}' | grep -Fxq "$SANDBOX"; then
    docker start "$SANDBOX" >/dev/null
    return 0
  fi

  docker run -d \
    --name "$SANDBOX" \
    -v "$REPO_ROOT:$REPO_ROOT" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -w "$REPO_ROOT" \
    "$TEMPLATE" \
    sleep infinity >/dev/null
}

ensure_sandbox

docker exec --user root "$SANDBOX" bash -lc '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y \
    build-essential \
    mold \
    libc6-dev \
    pkg-config \
    libwayland-dev \
    libx11-dev \
    libxkbcommon-dev \
    libasound2-dev \
    libudev-dev
'

if [[ "$SKIP_FETCH" -eq 0 ]]; then
  docker exec "$SANDBOX" bash -lc "cd $(printf '%q' "$REPO_ROOT") && cargo fetch --locked"
fi

docker exec "$SANDBOX" bash -lc '
  set -euo pipefail
  command -v gcc >/dev/null
  command -v c++ >/dev/null
  command -v mold >/dev/null
  dpkg -s libc6-dev >/dev/null
'

echo "Sandbox '$SANDBOX' is ready for Rust verification."
