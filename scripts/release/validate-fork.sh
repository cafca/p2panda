#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/release/validate-fork.sh preflight
  ./scripts/release/validate-fork.sh push-branch [branch]
  ./scripts/release/validate-fork.sh open-pr [branch]
  ./scripts/release/validate-fork.sh branch-status <branch> [commit-ish]
  ./scripts/release/validate-fork.sh pr-status <pr-number|branch>
  ./scripts/release/validate-fork.sh push-tag <vX.Y.Z-experimental.N>
  ./scripts/release/validate-fork.sh release-status <vX.Y.Z-experimental.N>
  ./scripts/release/validate-fork.sh cleanup-tag <vX.Y.Z-experimental.N>
  ./scripts/release/validate-fork.sh origin-status <commit-ish>

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
FORK_REPO="cafca/p2panda"
ORIGIN_REPO="p2panda/p2panda"

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

require_api_tools() {
  if ! command -v curl >/dev/null 2>&1; then
    echo "missing required tool: curl" >&2
    exit 1
  fi

  if ! command -v jq >/dev/null 2>&1; then
    echo "missing required tool: jq" >&2
    exit 1
  fi
}

api_get() {
  local path="$1"
  curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com${path}"
}

resolve_pr_json() {
  local selector="$1"

  if [[ "$selector" =~ ^[0-9]+$ ]]; then
    api_get "/repos/${FORK_REPO}/pulls/${selector}"
    return 0
  fi

  api_get "/repos/${FORK_REPO}/pulls?state=all&head=cafca:${selector}&per_page=100" |
    jq '.[0]'
}

resolve_branch_json() {
  local branch="$1"
  api_get "/repos/${FORK_REPO}/branches/${branch}"
}

print_check_runs() {
  local repo="$1"
  local sha="$2"

  api_get "/repos/${repo}/commits/${sha}/check-runs" |
    jq -r '
      if (.check_runs | length) == 0 then
        "  (no check runs found)"
      else
        .check_runs[]
        | "  - \(.name): status=\(.status) conclusion=\(.conclusion // "pending")"
      end
    '
}

check_runs_all_green() {
  local repo="$1"
  local sha="$2"

  api_get "/repos/${repo}/commits/${sha}/check-runs" |
    jq -e '
      (.total_count > 0) and
      all(
        .check_runs[];
        .status == "completed" and ((.conclusion // "") | IN("success", "neutral", "skipped"))
      )
    ' >/dev/null
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

release_run_json_for_tag() {
  local tag="$1"
  local head_sha

  if ! head_sha="$(api_get "/repos/${FORK_REPO}/commits/${tag}" 2>/dev/null | jq -r '.sha')" || [[ -z "$head_sha" || "$head_sha" == "null" ]]; then
    echo "tag '$tag' does not exist on ${FORK_REPO}" >&2
    return 1
  fi

  api_get "/repos/${FORK_REPO}/actions/workflows/release.yml/runs?event=push&per_page=100" |
    jq --arg sha "$head_sha" '
      .workflow_runs
      | map(select(.head_sha == $sha))
      | sort_by(.created_at)
      | last
    '
}

print_release_assets() {
  local release_json="$1"

  jq -r '
    if (.assets | length) == 0 then
      "  (no assets found)"
    else
      .assets[]
      | "  - \(.name)"
    end
  ' <<<"$release_json"
}

release_assets_complete() {
  local release_json="$1"

  jq -e '
    [
      any(.assets[]?; .name | endswith(".dmg")),
      any(.assets[]?; .name | endswith(".AppImage")),
      any(.assets[]?; .name | endswith(".tar.gz")),
      any(.assets[]?; .name | endswith(".msi")),
      (([.assets[]? | select(.name | endswith(".zip"))] | length) >= 2),
      (([.assets[]? | select(.name | endswith(".sha256"))] | length) >= 5)
    ]
    | all(.[])
  ' <<<"$release_json" >/dev/null
}

release_jobs_complete() {
  local run_id="$1"

  api_get "/repos/${FORK_REPO}/actions/runs/${run_id}/jobs" |
    jq -e '
      [
        any(.jobs[]?; .name == "macos" and .status == "completed" and .conclusion == "success"),
        any(.jobs[]?; .name == "linux" and .status == "completed" and .conclusion == "success"),
        any(.jobs[]?; .name == "windows" and .status == "completed" and .conclusion == "success"),
        any(.jobs[]?; .name == "release" and .status == "completed" and .conclusion == "success")
      ]
      | all(.[])
    ' >/dev/null
}

cmd_preflight() {
  assert_safe_remotes

  local branch fork_url origin_url head_sha
  branch="$(current_branch)"
  fork_url="$(remote_url fork)"
  origin_url="$(remote_url origin)"
  head_sha="$(git rev-parse HEAD)"

  echo "fork remote:   $fork_url"
  echo "origin remote: $origin_url"
  echo "branch:        $branch"
  echo "local head:    $head_sha"

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
      echo "push path:     available via SSH"
    else
      echo "ssh client:    missing"
      echo "push path:     blocked (fork remote requires SSH)"
    fi
  else
    echo "ssh client:    not required"
    echo "push path:     available without SSH client"
  fi
}

cmd_branch_status() {
  local branch="${1:-}"
  local expected_commitish="${2:-HEAD}"
  local branch_json remote_sha expected_sha branch_url

  if [[ -z "$branch" ]]; then
    echo "missing branch argument" >&2
    usage
    exit 1
  fi

  assert_safe_remotes
  require_api_tools

  if ! expected_sha="$(git rev-parse "${expected_commitish}^{commit}" 2>/dev/null)"; then
    echo "unknown commit-ish: $expected_commitish" >&2
    exit 1
  fi

  if ! branch_json="$(resolve_branch_json "$branch" 2>/dev/null)"; then
    echo "failed to query branch '$branch' on ${FORK_REPO}" >&2
    exit 1
  fi

  remote_sha="$(jq -r '.commit.sha // empty' <<<"$branch_json")"
  branch_url="$(jq -r '.commit.html_url // empty' <<<"$branch_json")"

  if [[ -z "$remote_sha" ]]; then
    echo "branch '$branch' does not exist on ${FORK_REPO}" >&2
    exit 1
  fi

  echo "repo:          ${FORK_REPO}"
  echo "branch:        ${branch}"
  echo "fork head:     ${remote_sha}"
  echo "expected sha:  ${expected_sha}"
  if [[ -n "$branch_url" ]]; then
    echo "fork commit:   ${branch_url}"
  fi

  if [[ "$remote_sha" == "$expected_sha" ]]; then
    echo "status:        up to date"
    return 0
  fi

  echo "status:        stale (fork branch does not match ${expected_commitish})" >&2
  return 1
}

cmd_pr_status() {
  local selector="${1:-}"
  local pr_json pr_number title state draft head_ref head_sha pr_url

  if [[ -z "$selector" ]]; then
    echo "missing PR selector (number or branch)" >&2
    usage
    exit 1
  fi

  assert_safe_remotes
  require_api_tools

  if ! pr_json="$(resolve_pr_json "$selector" 2>/dev/null)"; then
    echo "failed to query PR data from ${FORK_REPO}" >&2
    exit 1
  fi

  if [[ "$(jq -r 'type' <<<"$pr_json")" == "null" ]]; then
    echo "no PR found for selector '$selector' on ${FORK_REPO}" >&2
    exit 1
  fi

  pr_number="$(jq -r '.number' <<<"$pr_json")"
  title="$(jq -r '.title' <<<"$pr_json")"
  state="$(jq -r '.state' <<<"$pr_json")"
  draft="$(jq -r '.draft' <<<"$pr_json")"
  head_ref="$(jq -r '.head.ref' <<<"$pr_json")"
  head_sha="$(jq -r '.head.sha' <<<"$pr_json")"
  pr_url="$(jq -r '.html_url' <<<"$pr_json")"

  echo "repo:        ${FORK_REPO}"
  echo "pr:          #${pr_number}"
  echo "title:       ${title}"
  echo "url:         ${pr_url}"
  echo "state:       ${state}"
  echo "draft:       ${draft}"
  echo "head ref:    ${head_ref}"
  echo "head sha:    ${head_sha}"
  echo "check runs:"
  print_check_runs "$FORK_REPO" "$head_sha"

  if [[ "$state" != "open" || "$draft" != "false" ]]; then
    echo "PR is not ready for validation yet" >&2
    exit 1
  fi

  check_runs_all_green "$FORK_REPO" "$head_sha"
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

cmd_release_status() {
  local tag="${1:-}"
  local run_json run_id run_url run_status run_conclusion release_json release_url

  if [[ -z "$tag" ]]; then
    echo "missing tag argument" >&2
    usage
    exit 1
  fi

  assert_safe_remotes
  assert_experimental_tag "$tag"
  require_api_tools

  if ! run_json="$(release_run_json_for_tag "$tag")"; then
    exit 1
  fi

  if [[ "$(jq -r 'type' <<<"$run_json")" == "null" ]]; then
    echo "no release workflow run found for tag '$tag' on ${FORK_REPO}" >&2
    exit 1
  fi

  run_id="$(jq -r '.id' <<<"$run_json")"
  run_url="$(jq -r '.html_url' <<<"$run_json")"
  run_status="$(jq -r '.status' <<<"$run_json")"
  run_conclusion="$(jq -r '.conclusion // "pending"' <<<"$run_json")"

  echo "repo:        ${FORK_REPO}"
  echo "tag:         ${tag}"
  echo "run id:      ${run_id}"
  echo "run status:  ${run_status}"
  echo "conclusion:  ${run_conclusion}"
  echo "run url:     ${run_url}"
  echo "jobs:"
  api_get "/repos/${FORK_REPO}/actions/runs/${run_id}/jobs" |
    jq -r '
      if (.jobs | length) == 0 then
        "  (no jobs found)"
      else
        .jobs[]
        | "  - \(.name): status=\(.status) conclusion=\(.conclusion // "pending")"
      end
    '

  if ! release_json="$(api_get "/repos/${FORK_REPO}/releases/tags/${tag}" 2>/dev/null)"; then
    echo "no GitHub release found for tag '$tag' on ${FORK_REPO}" >&2
    exit 1
  fi

  release_url="$(jq -r '.html_url' <<<"$release_json")"
  echo "release url: ${release_url}"
  echo "assets:"
  print_release_assets "$release_json"

  [[ "$run_status" == "completed" && "$run_conclusion" == "success" ]] &&
    release_jobs_complete "$run_id" &&
    release_assets_complete "$release_json"
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

cmd_origin_status() {
  local commitish="${1:-HEAD}"
  local sha

  assert_safe_remotes
  require_api_tools

  if ! sha="$(git rev-parse "$commitish^{commit}" 2>/dev/null)"; then
    echo "unknown commit-ish: $commitish" >&2
    exit 1
  fi

  echo "repo:     ${ORIGIN_REPO}"
  echo "commit:   ${sha}"
  echo "runs:"

  api_get "/repos/${ORIGIN_REPO}/actions/runs?per_page=100" |
    jq -r --arg sha "$sha" '
      [
        .workflow_runs[]
        | select(.head_sha == $sha)
        | "  - \(.name): event=\(.event) status=\(.status) conclusion=\(.conclusion // "pending")"
      ] as $runs
      | if ($runs | length) == 0 then
          "  (no runs found)"
        else
          $runs[]
        end
    '

  ! api_get "/repos/${ORIGIN_REPO}/actions/runs?per_page=100" |
    jq -e --arg sha "$sha" 'any(.workflow_runs[]?; .head_sha == $sha)' >/dev/null
}

COMMAND="${1:-}"
case "$COMMAND" in
  preflight)
    shift
    cmd_preflight "$@"
    ;;
  branch-status)
    shift
    cmd_branch_status "$@"
    ;;
  push-branch)
    shift
    cmd_push_branch "$@"
    ;;
  open-pr)
    shift
    cmd_open_pr "$@"
    ;;
  pr-status)
    shift
    cmd_pr_status "$@"
    ;;
  push-tag)
    shift
    cmd_push_tag "$@"
    ;;
  release-status)
    shift
    cmd_release_status "$@"
    ;;
  cleanup-tag)
    shift
    cmd_cleanup_tag "$@"
    ;;
  origin-status)
    shift
    cmd_origin_status "$@"
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
