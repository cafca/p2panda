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

python3 - "$TAG" "$VERSION" <<'PYEOF'
import re, sys

def parse_semver(v):
    # Official semver regex from https://semver.org/#is-there-a-suggested-regexp-to-check-a-semver-string
    m = re.fullmatch(
        r'^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)'
        r'(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?'
        r'(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$', v)
    return m

tag, version = sys.argv[1], sys.argv[2]
tag_no_v = tag[1:] if tag.startswith('v') else tag

m = parse_semver(tag_no_v)
if not m:
    print(f"invalid semver tag: '{tag}'", file=sys.stderr)
    sys.exit(1)

core = f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
if core != version:
    print(f"tag/version mismatch: tag has {core} but Cargo.toml has {version}", file=sys.stderr)
    sys.exit(1)

pre = m.group(4) or ''
if pre and not pre.startswith('experimental'):
    print(f"pre-release identifier must start with 'experimental', got: '{pre}'", file=sys.stderr)
    sys.exit(1)

print(version)
PYEOF
