#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/verify-in-sandbox.sh [--provider claude|codex] [--sandbox name] [--template image] [--bootstrap] -- <command...>

Run a non-interactive verification command inside the long-lived Docker sandbox.

Options:
  -p, --provider  Sandbox flavor. Defaults to codex.
      --sandbox   Override the container name. Defaults to <provider>-p2panda.
      --template  Override the image passed through to bootstrap.
      --bootstrap Prepare the sandbox before running the command.
  -h, --help      Show this help text.
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROVIDER="codex"
SANDBOX=""
TEMPLATE=""
BOOTSTRAP=0

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
    --bootstrap)
      BOOTSTRAP=1
      shift
      ;;
    --)
      shift
      break
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

if [[ $# -eq 0 ]]; then
  echo "Missing verification command." >&2
  usage
  exit 1
fi

SANDBOX="${SANDBOX:-${PROVIDER}-p2panda}"

if [[ "$BOOTSTRAP" -eq 1 ]]; then
  BOOTSTRAP_ARGS=(-p "$PROVIDER" --sandbox "$SANDBOX")
  if [[ -n "$TEMPLATE" ]]; then
    BOOTSTRAP_ARGS+=(--template "$TEMPLATE")
  fi
  "$REPO_ROOT/scripts/bootstrap-sandbox-toolchain.sh" "${BOOTSTRAP_ARGS[@]}"
fi

if ! docker ps --format '{{.Names}}' | grep -Fxq "$SANDBOX"; then
  echo "Sandbox '$SANDBOX' is not running. Run scripts/bootstrap-sandbox-toolchain.sh first or pass --bootstrap." >&2
  exit 1
fi

CMD="$(printf '%q ' "$@")"
exec docker exec -i "$SANDBOX" bash -lc "cd $(printf '%q' "$REPO_ROOT") && $CMD"
