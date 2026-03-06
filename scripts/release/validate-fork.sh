#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/release/validate-fork.sh preflight
  ./scripts/release/validate-fork.sh summary <branch> [pr-number|branch] [tag] [commit-ish]
  ./scripts/release/validate-fork.sh push-branch [branch]
  ./scripts/release/validate-fork.sh open-pr [branch]
  ./scripts/release/validate-fork.sh ready-pr [pr-number|branch]
  ./scripts/release/validate-fork.sh branch-status <branch> [commit-ish]
  ./scripts/release/validate-fork.sh pr-status <pr-number|branch> [commit-ish]
  ./scripts/release/validate-fork.sh wait-pr <pr-number|branch> [commit-ish] [timeout-seconds] [poll-seconds]
  ./scripts/release/validate-fork.sh push-tag <vX.Y.Z-experimental.N>
  ./scripts/release/validate-fork.sh release-status <vX.Y.Z-experimental.N>
  ./scripts/release/validate-fork.sh wait-release <vX.Y.Z-experimental.N> [timeout-seconds] [poll-seconds]
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
FORK_HTTPS_PUSH_URL="https://github.com/cafca/p2panda.git"
DEFAULT_WAIT_TIMEOUT_SECONDS=900
DEFAULT_WAIT_POLL_SECONDS=15

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

github_token_source() {
  if [[ -n "${GITHUB_TOKEN_FILE:-}" ]]; then
    printf '%s\n' "GITHUB_TOKEN_FILE"
    return 0
  fi

  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    printf '%s\n' "GITHUB_TOKEN"
    return 0
  fi

  if [[ -n "${GH_TOKEN:-}" ]]; then
    printf '%s\n' "GH_TOKEN"
    return 0
  fi

  return 1
}

read_github_token() {
  if [[ -n "${GITHUB_TOKEN_FILE:-}" ]]; then
    if [[ ! -f "${GITHUB_TOKEN_FILE}" ]]; then
      echo "GITHUB_TOKEN_FILE does not exist: ${GITHUB_TOKEN_FILE}" >&2
      return 1
    fi

    tr -d '\r\n' <"${GITHUB_TOKEN_FILE}"
    return 0
  fi

  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    printf '%s' "${GITHUB_TOKEN}"
    return 0
  fi

  if [[ -n "${GH_TOKEN:-}" ]]; then
    printf '%s' "${GH_TOKEN}"
    return 0
  fi

  return 1
}

has_github_token() {
  read_github_token >/dev/null 2>&1
}

setup_github_api_netrc() {
  local token
  token="$(read_github_token)"

  GITHUB_API_AUTH_DIR="$(mktemp -d)"
  GITHUB_API_AUTH_NETRC="${GITHUB_API_AUTH_DIR}/netrc"
  cat >"${GITHUB_API_AUTH_NETRC}" <<EOF
machine api.github.com
  login x-access-token
  password ${token}
EOF
  chmod 600 "${GITHUB_API_AUTH_NETRC}"
}

cleanup_github_api_netrc() {
  if [[ -n "${GITHUB_API_AUTH_DIR:-}" && -d "${GITHUB_API_AUTH_DIR}" ]]; then
    rm -rf "${GITHUB_API_AUTH_DIR}"
  fi
  unset GITHUB_API_AUTH_DIR
  unset GITHUB_API_AUTH_NETRC
}

setup_github_git_askpass() {
  local token
  token="$(read_github_token)"

  GITHUB_GIT_AUTH_DIR="$(mktemp -d)"
  GITHUB_GIT_ASKPASS="${GITHUB_GIT_AUTH_DIR}/askpass.sh"
  cat >"${GITHUB_GIT_ASKPASS}" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  *Username*)
    printf '%s\n' "x-access-token"
    ;;
  *Password*)
    printf '%s\n' "${GITHUB_TOKEN_FOR_GIT_PUSH:-}"
    ;;
  *)
    printf '\n'
    ;;
esac
EOF
  chmod 700 "${GITHUB_GIT_ASKPASS}"
  GITHUB_TOKEN_FOR_GIT_PUSH="${token}"
}

cleanup_github_git_askpass() {
  if [[ -n "${GITHUB_GIT_AUTH_DIR:-}" && -d "${GITHUB_GIT_AUTH_DIR}" ]]; then
    rm -rf "${GITHUB_GIT_AUTH_DIR}"
  fi
  unset GITHUB_GIT_AUTH_DIR
  unset GITHUB_GIT_ASKPASS
  unset GITHUB_TOKEN_FOR_GIT_PUSH
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

  if has_github_token; then
    return 0
  fi

  if [[ "$fork_url" == git@* || "$fork_url" == ssh://* ]]; then
    if ! command -v ssh >/dev/null 2>&1; then
      echo "fork remote uses SSH but neither an ssh client nor GITHUB_TOKEN/GITHUB_TOKEN_FILE/GH_TOKEN is available" >&2
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

require_token_auth() {
  if ! has_github_token; then
    echo "GitHub token auth is required; set GITHUB_TOKEN, GH_TOKEN, or GITHUB_TOKEN_FILE" >&2
    exit 1
  fi
}

api_request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local -a args
  local status

  args=(
    curl
    -fsSL
    -X "$method"
    -H "Accept: application/vnd.github+json"
  )

  if has_github_token; then
    setup_github_api_netrc
    args+=(--netrc-file "${GITHUB_API_AUTH_NETRC}")
  fi

  if [[ -n "$body" ]]; then
    args+=(-H "Content-Type: application/json" --data "$body")
  fi

  args+=("https://api.github.com${path}")
  "${args[@]}"
  status=$?

  cleanup_github_api_netrc
  return "$status"
}

api_get() {
  api_request GET "$1"
}

api_post() {
  api_request POST "$1" "$2"
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
        | "  - \(.name): status=\(.status) conclusion=\(.conclusion // "pending") url=\(.details_url // .html_url // "n/a")"
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

git_push_fork() {
  local -a refspecs=("$@")
  local status

  if has_github_token; then
    setup_github_git_askpass
    GIT_ASKPASS="${GITHUB_GIT_ASKPASS}" \
      GIT_TERMINAL_PROMPT=0 \
      GITHUB_TOKEN_FOR_GIT_PUSH="${GITHUB_TOKEN_FOR_GIT_PUSH}" \
      git -c credential.helper= push "${FORK_HTTPS_PUSH_URL}" "${refspecs[@]}"
    status=$?
    cleanup_github_git_askpass
    return "$status"
  fi

  git push fork "${refspecs[@]}"
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

now_epoch() {
  date +%s
}

wait_with_polling() {
  local description="$1"
  local timeout_seconds="$2"
  local poll_seconds="$3"
  shift 3

  local started_at deadline attempt output status

  if ! [[ "$timeout_seconds" =~ ^[0-9]+$ ]] || [[ "$timeout_seconds" -le 0 ]]; then
    echo "timeout must be a positive integer number of seconds" >&2
    exit 1
  fi

  if ! [[ "$poll_seconds" =~ ^[0-9]+$ ]] || [[ "$poll_seconds" -le 0 ]]; then
    echo "poll interval must be a positive integer number of seconds" >&2
    exit 1
  fi

  started_at="$(now_epoch)"
  deadline=$((started_at + timeout_seconds))
  attempt=1

  echo "waiting for ${description} (timeout=${timeout_seconds}s poll=${poll_seconds}s)"

  while true; do
    if output="$("$@" 2>&1)"; then
      printf '[attempt %d] ready\n' "$attempt"
      printf '%s\n' "$output"
      return 0
    fi

    status=$?
    printf '[attempt %d] not ready yet\n' "$attempt" >&2
    printf '%s\n' "$output" >&2

    if [[ "$(now_epoch)" -ge "$deadline" ]]; then
      echo "timed out waiting for ${description}" >&2
      return "$status"
    fi

    attempt=$((attempt + 1))
    sleep "$poll_seconds"
  done
}

cmd_preflight() {
  assert_safe_remotes

  local branch fork_url origin_url head_sha auth_source
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

  if auth_source="$(github_token_source 2>/dev/null)"; then
    echo "token auth:    ok (${auth_source})"
  else
    echo "token auth:    missing"
  fi

  if [[ "$fork_url" == git@* || "$fork_url" == ssh://* ]]; then
    if command -v ssh >/dev/null 2>&1; then
      echo "ssh client:    ok"
      if has_github_token; then
        echo "push path:     available via SSH or HTTPS token"
      else
        echo "push path:     available via SSH"
      fi
    else
      echo "ssh client:    missing"
      if has_github_token; then
        echo "push path:     available via HTTPS token"
      else
        echo "push path:     blocked (fork remote requires SSH)"
      fi
    fi
  else
    echo "ssh client:    not required"
    if has_github_token; then
      echo "push path:     available via HTTPS token"
    else
      echo "push path:     available via fork remote credentials"
    fi
  fi

  if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
    echo "pr path:       available via gh"
  elif has_github_token; then
    echo "pr path:       available via GitHub API token"
  else
    echo "pr path:       blocked (requires gh auth or token)"
  fi
}

cmd_summary() {
  local branch="${1:-}"
  local pr_selector="${2:-}"
  local release_tag="${3:-}"
  local expected_commitish="${4:-HEAD}"
  local failures=0

  if [[ -z "$branch" ]]; then
    echo "missing branch argument" >&2
    usage
    exit 1
  fi

  if [[ -z "$pr_selector" ]]; then
    pr_selector="$branch"
  fi

  echo "== preflight =="
  if ! (cmd_preflight); then
    failures=$((failures + 1))
  fi
  echo

  echo "== fork branch =="
  if ! (cmd_branch_status "$branch" "$expected_commitish"); then
    failures=$((failures + 1))
  fi
  echo

  echo "== fork PR =="
  if ! (cmd_pr_status "$pr_selector" "$expected_commitish"); then
    failures=$((failures + 1))
  fi
  echo

  if [[ -n "$release_tag" ]]; then
    echo "== release =="
    if ! (cmd_release_status "$release_tag"); then
      failures=$((failures + 1))
    fi
    echo
  fi

  echo "== upstream safety =="
  if ! (cmd_origin_status "$expected_commitish"); then
    failures=$((failures + 1))
  fi

  if [[ "$failures" -eq 0 ]]; then
    echo
    echo "summary: all checked Task 41 validation surfaces are currently green"
    return 0
  fi

  echo
  echo "summary: ${failures} validation check(s) need attention" >&2
  return 1
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
  local expected_commitish="${2:-HEAD}"
  local pr_json pr_number title state draft head_ref head_sha pr_url expected_sha

  if [[ -z "$selector" ]]; then
    echo "missing PR selector (number or branch)" >&2
    usage
    exit 1
  fi

  assert_safe_remotes
  require_api_tools

  if ! expected_sha="$(git rev-parse "${expected_commitish}^{commit}" 2>/dev/null)"; then
    echo "unknown commit-ish: $expected_commitish" >&2
    exit 1
  fi

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
  echo "expected:    ${expected_sha} (${expected_commitish})"
  echo "check runs:"
  print_check_runs "$FORK_REPO" "$head_sha"

  if [[ "$state" != "open" || "$draft" != "false" ]]; then
    echo "PR is not ready for validation yet" >&2
    exit 1
  fi

  if [[ "$head_sha" != "$expected_sha" ]]; then
    echo "PR head does not match ${expected_commitish}; push the latest commit to the fork before treating this PR as valid" >&2
    exit 1
  fi

  check_runs_all_green "$FORK_REPO" "$head_sha"
}

cmd_wait_pr() {
  local selector="${1:-}"
  local expected_commitish="${2:-HEAD}"
  local timeout_seconds="${3:-$DEFAULT_WAIT_TIMEOUT_SECONDS}"
  local poll_seconds="${4:-$DEFAULT_WAIT_POLL_SECONDS}"

  if [[ -z "$selector" ]]; then
    echo "missing PR selector (number or branch)" >&2
    usage
    exit 1
  fi

  wait_with_polling "PR ${selector} on ${FORK_REPO}" "$timeout_seconds" "$poll_seconds" cmd_pr_status "$selector" "$expected_commitish"
}

cmd_push_branch() {
  local branch="${1:-$(current_branch)}"

  assert_safe_remotes
  require_push_transport

  git_push_fork "HEAD:refs/heads/$branch"
}

cmd_open_pr() {
  local branch="${1:-$(current_branch)}"
  local pr_json title body payload pr_url

  assert_safe_remotes

  if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
    gh pr create \
      --repo cafca/p2panda \
      --base main \
      --head "cafca:$branch" \
      --fill
    return 0
  fi

  require_api_tools
  require_token_auth

  if pr_json="$(resolve_pr_json "$branch" 2>/dev/null)" && [[ "$(jq -r 'type' <<<"$pr_json")" != "null" ]]; then
    pr_url="$(jq -r '.html_url // empty' <<<"$pr_json")"
    if [[ -n "$pr_url" ]]; then
      echo "PR already exists: $pr_url"
      return 0
    fi
  fi

  title="$(git log -1 --pretty=%s)"
  body="$(git log -1 --pretty=%b)"
  if [[ -z "$body" ]]; then
    body="Automated fork validation PR for branch ${branch}."
  fi

  payload="$(jq -n \
    --arg title "$title" \
    --arg head "cafca:${branch}" \
    --arg base "main" \
    --arg body "$body" \
    '{title: $title, head: $head, base: $base, body: $body, draft: false, maintainer_can_modify: true}')"

  pr_json="$(api_post "/repos/${FORK_REPO}/pulls" "$payload")"
  pr_url="$(jq -r '.html_url // empty' <<<"$pr_json")"

  if [[ -z "$pr_url" ]]; then
    echo "failed to create PR on ${FORK_REPO}" >&2
    exit 1
  fi

  echo "$pr_url"
}

cmd_ready_pr() {
  local selector="${1:-$(current_branch)}"
  local pr_json pr_number state draft pr_url

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
  state="$(jq -r '.state' <<<"$pr_json")"
  draft="$(jq -r '.draft' <<<"$pr_json")"
  pr_url="$(jq -r '.html_url // empty' <<<"$pr_json")"

  if [[ "$state" != "open" ]]; then
    echo "PR #${pr_number} is not open; current state: ${state}" >&2
    exit 1
  fi

  if [[ "$draft" == "false" ]]; then
    if [[ -n "$pr_url" ]]; then
      echo "PR already ready for review: ${pr_url}"
    else
      echo "PR #${pr_number} is already ready for review"
    fi
    return 0
  fi

  if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
    gh api \
      --method PATCH \
      -H "Accept: application/vnd.github+json" \
      "/repos/${FORK_REPO}/pulls/${pr_number}" \
      -f draft=false >/dev/null
  else
    require_token_auth
    api_request PATCH "/repos/${FORK_REPO}/pulls/${pr_number}" '{"draft":false}' >/dev/null
  fi

  if [[ -n "$pr_url" ]]; then
    echo "PR ready for review: ${pr_url}"
  else
    echo "PR #${pr_number} marked ready for review"
  fi
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

  git_push_fork "refs/tags/$tag"
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

cmd_wait_release() {
  local tag="${1:-}"
  local timeout_seconds="${2:-$DEFAULT_WAIT_TIMEOUT_SECONDS}"
  local poll_seconds="${3:-$DEFAULT_WAIT_POLL_SECONDS}"

  if [[ -z "$tag" ]]; then
    echo "missing tag argument" >&2
    usage
    exit 1
  fi

  wait_with_polling "release validation for ${tag} on ${FORK_REPO}" "$timeout_seconds" "$poll_seconds" cmd_release_status "$tag"
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

  git_push_fork ":refs/tags/$tag"
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
  summary)
    shift
    cmd_summary "$@"
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
  ready-pr)
    shift
    cmd_ready_pr "$@"
    ;;
  pr-status)
    shift
    cmd_pr_status "$@"
    ;;
  wait-pr)
    shift
    cmd_wait_pr "$@"
    ;;
  push-tag)
    shift
    cmd_push_tag "$@"
    ;;
  release-status)
    shift
    cmd_release_status "$@"
    ;;
  wait-release)
    shift
    cmd_wait_release "$@"
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
