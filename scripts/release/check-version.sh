#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CRATE_MANIFEST="$ROOT_DIR/file-sharing/Cargo.toml"

if [[ ! -f "$CRATE_MANIFEST" ]]; then
  echo "missing manifest: $CRATE_MANIFEST" >&2
  exit 1
fi

VERSION="$(
  awk -F'"' '
    /^\[package\]/ { in_package = 1; next }
    /^\[/ && in_package { in_package = 0 }
    in_package && /^version = / { print $2; exit }
  ' "$CRATE_MANIFEST"
)"

if [[ -z "$VERSION" ]]; then
  echo "failed to read version from $CRATE_MANIFEST" >&2
  exit 1
fi

TAG="${1:-${GITHUB_REF_NAME:-}}"
if [[ -z "$TAG" ]]; then
  echo "missing tag; pass as first arg or set GITHUB_REF_NAME" >&2
  exit 1
fi

EXPECTED_TAG="v$VERSION"
if [[ "$TAG" != "$EXPECTED_TAG" ]]; then
  echo "tag/version mismatch: expected '$EXPECTED_TAG' but got '$TAG'" >&2
  exit 1
fi

echo "$VERSION"
