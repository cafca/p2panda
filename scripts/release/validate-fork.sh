#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/release/validate-fork.sh preflight
  ./scripts/release/validate-fork.sh push-branch [branch]
  ./scripts/release/validate-fork.sh open-pr [branch]
  ./scripts/release/validate-fork.sh push-tag <vX.Y.Z-experimental.N>
  ./scripts/release/validate-fork.sh cleanup-tag <vX.Y.Z-experimental.N>

Safety rules:
  - only the `fork` remote may be used for pushes and tag operations
  - `fork` must point at cafca/p2panda
  - `origin` must still point at p2panda/p2panda
  - release validation tags must use the experimental format accepted by
    scripts/release/check-version.sh
EOF
}

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
CHECK_VERSION_SCRIPT="$ROOT_DIR/scripts/release/check-version.sh"

EXPECTED_FORK_URLS=(
  "git@github.com:cafca/p2panda.git"
  "https://github.com/cafca/p2panda.git"
)
EXPECTED_ORIGIN_URLS=(
  "git@github.com:p2panda/p2panda.git"
  "https://github.com/p2panda/p2panda.git"
)

matches_expected_url() {
  local actual="$1"
  shift

  local expected
  for expected in "$@"; do
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
  done

  return 1
}

remote_url() {
  git remote get-url "$1" 2>/dev/null || true
}

assert_safe_remotes() {
  local fork_url origin_url
  fork_url="$(remote_url fork)"
  origin_url="$(remote_url origin)"

  if [[ -z "$fork_url" ]]; then
    echo "missing git remote: fork" >&2
    exit 1
  fi

  if [[ -z "$origin_url" ]]; then
    echo "missing git remote: origin" >&2
    exit 1
  fi

  if ! matches_expected_url "$fork_url" "${EXPECTED_FORK_URLS[@]}"; then
    echo "unsafe fork remote: expected cafca/p2panda but found '$fork_url'" >&2
    exit 1
  fi

  if ! matches_expected_url "$origin_url" "${EXPECTED_ORIGIN_URLS[@]}"; then
    echo "unsafe origin remote: expected p2panda/p2panda but found '$origin_url'" >&2
    exit 1
  fi

  if [[ "$fork_url" == "$origin_url" ]]; then
    echo "fork and origin remotes must not point at the same repository" >&2
    exit 1
  fi
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

require_gh() {
  if ! command -v gh >/dev/null 2>&1; then
    echo "missing required tool: gh" >&2
    exit 1
  fi

  if ! gh auth status >/dev/null 2>&1; then
    echo "GitHub CLI is not authenticated; run 'gh auth login' first" >&2
    exit 1
  fi
}

require_push_transport() {
  local fork_url
  fork_url="$(remote_url fork)"

  if [[ "$fork_url" == git@* || "$fork_url" == ssh://* ]]; then
    if ! command -v ssh >/dev/null 2>&1; then
      echo "fork remote uses SSH but no ssh client is available" >&2
      exit 1
    fi
  fi
}

assert_experimental_tag() {
  local tag="$1"

  "$CHECK_VERSION_SCRIPT" "$tag" >/dev/null

  if [[ ! "$tag" =~ -experimental\.[0-9]+$ ]]; then
    echo "release validation tags must use the experimental format: vX.Y.Z-experimental.N" >&2
    exit 1
  fi
}

ensure_local_tag_points_to_head() {
  local tag="$1"
  local head_sha tag_sha
  head_sha="$(git rev-parse HEAD)"

  if git rev-parse "$tag^{tag}" >/dev/null 2>&1 || git rev-parse "$tag^{commit}" >/dev/null 2>&1; then
    tag_sha="$(git rev-parse "$tag^{commit}")"
    if [[ "$tag_sha" != "$head_sha" ]]; then
      echo "existing local tag '$tag' does not point at HEAD" >&2
      exit 1
    fi
    return 0
  fi

  git tag -a "$tag" -m "Experimental release validation $tag" HEAD
}

cmd_preflight() {
  assert_safe_remotes

  local branch fork_url origin_url
  branch="$(current_branch)"
  fork_url="$(remote_url fork)"
  origin_url="$(remote_url origin)"

  echo "fork remote:   $fork_url"
  echo "origin remote: $origin_url"
  echo "branch:        $branch"

  if command -v gh >/dev/null 2>&1; then
    if gh auth status >/dev/null 2>&1; then
      echo "gh auth:       ok"
    else
      echo "gh auth:       missing"
    fi
  else
    echo "gh auth:       gh not installed"
  fi

  if [[ "$fork_url" == git@* || "$fork_url" == ssh://* ]]; then
    if command -v ssh >/dev/null 2>&1; then
      echo "ssh client:    ok"
    else
      echo "ssh client:    missing"
    fi
  else
    echo "ssh client:    not required"
  fi
}

cmd_push_branch() {
  local branch="${1:-$(current_branch)}"

  assert_safe_remotes
  require_push_transport

  git push fork "HEAD:refs/heads/$branch"
}

cmd_open_pr() {
  local branch="${1:-$(current_branch)}"

  assert_safe_remotes
  require_gh

  gh pr create \
    --repo cafca/p2panda \
    --base main \
    --head "cafca:$branch" \
    --fill
}

cmd_push_tag() {
  local tag="${1:-}"

  if [[ -z "$tag" ]]; then
    echo "missing tag argument" >&2
    usage
    exit 1
  fi

  assert_safe_remotes
  assert_experimental_tag "$tag"
  require_push_transport
  ensure_local_tag_points_to_head "$tag"

  git push fork "refs/tags/$tag"
}

cmd_cleanup_tag() {
  local tag="${1:-}"

  if [[ -z "$tag" ]]; then
    echo "missing tag argument" >&2
    usage
    exit 1
  fi

  assert_safe_remotes
  assert_experimental_tag "$tag"
  require_push_transport

  git push fork ":refs/tags/$tag"
  if git rev-parse "$tag^{commit}" >/dev/null 2>&1; then
    git tag -d "$tag"
  fi
}

COMMAND="${1:-}"
case "$COMMAND" in
  preflight)
    shift
    cmd_preflight "$@"
    ;;
  push-branch)
    shift
    cmd_push_branch "$@"
    ;;
  open-pr)
    shift
    cmd_open_pr "$@"
    ;;
  push-tag)
    shift
    cmd_push_tag "$@"
    ;;
  cleanup-tag)
    shift
    cmd_cleanup_tag "$@"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "unknown command: ${COMMAND:-<none>}" >&2
    usage
    exit 1
    ;;
esac
