#!/usr/bin/env bash
# ralph.sh
# Usage: ./ralph.sh [--provider claude|codex] [--sandbox name] [--template image] <iterations>

set -euo pipefail

PROMPT='@PRD.jsonl @progress.txt
PRD.jsonl is JSON Lines (one JSON object per line). Fields: id, title, description, depends_on (array of ids), spec (object), acceptance (array of strings), passes (bool). Use jq -s to query it (e.g. jq -s ".[] | select(.passes==false)" PRD.jsonl).
1. Decide which task to work on next.
This should be the one YOU decide has the highest priority,
- not necessarily the first in the list.
2. Check any feedback loops, such as types and tests.
3. Append your progress to the progress.txt file. To get the current time, run: curl -sI https://www.google.com | grep -i "^date:" | cut -d" " -f2- (do NOT trust the container system clock).
4. Update the PRD.jsonl file and make sure to ONLY change the `passes` field of a SINGLE task. Do not make any other edits.
Set `passes` to `true` only if that tasks acceptance criteria actually passed in this run.
If verification is blocked or incomplete, leave `passes` as `false` and record the blocker in progress.txt.
If you discover a missing infrastructure/task that should be added to PRD.jsonl, do not edit PRD.jsonl beyond the single `passes` field change;
instead, record the learning clearly in progress.txt for a manual PRD amendment later.
5. Make a git commit of that feature.
ONLY WORK ON A SINGLE FEATURE.
If, while implementing the feature, you notice that all work
is complete, output <promise>COMPLETE</promise>.'

usage() {
  cat <<'EOF'
Usage: ./ralph.sh [--provider claude|codex] [--sandbox name] [--template image] <iterations>

Options:
  -p, --provider  Agent CLI to run inside the sandbox. Defaults to claude.
      --sandbox   Override the sandbox name. Defaults to <provider>-p2panda.
      --template  Override the sandbox template image. Defaults to <provider>-p2panda-template:latest.
  -h, --help      Show this help text.
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROVIDER="claude"
SANDBOX=""
TEMPLATE=""
ITERATIONS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--provider)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for $1" >&2
        usage
        exit 1
      fi
      PROVIDER="$2"
      shift 2
      ;;
    --sandbox)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for $1" >&2
        usage
        exit 1
      fi
      SANDBOX="$2"
      shift 2
      ;;
    --template)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for $1" >&2
        usage
        exit 1
      fi
      TEMPLATE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
    *)
      if [[ -n "$ITERATIONS" ]]; then
        echo "Unexpected extra argument: $1" >&2
        usage
        exit 1
      fi
      ITERATIONS="$1"
      shift
      ;;
  esac
done

if [[ -z "$ITERATIONS" ]]; then
  usage
  exit 1
fi

case "$PROVIDER" in
  claude)
    SANDBOX="${SANDBOX:-claude-p2panda}"
    TEMPLATE="${TEMPLATE:-claude-p2panda-template:latest}"
    AGENT_ARGS=(--print --verbose)
    ;;
  codex)
    SANDBOX="${SANDBOX:-codex-p2panda}"
    TEMPLATE="${TEMPLATE:-codex-p2panda-template:latest}"
    AGENT_ARGS=(exec --color never)
    ;;
  *)
    echo "Unsupported provider: $PROVIDER" >&2
    usage
    exit 1
    ;;
esac

ensure_sandbox() {
  if docker sandbox ls -q 2>/dev/null | grep -Fxq "$SANDBOX"; then
    return 0
  fi
  docker sandbox create \
    --name "$SANDBOX" \
    -t "$TEMPLATE" \
    "$PROVIDER" \
    "$REPO_ROOT"
}

set -x

for ((i=1; i<=ITERATIONS; i++)); do
  echo "======================================"
  echo "Starting iteration $i of $ITERATIONS with provider '$PROVIDER'"
  echo "Sandbox: $SANDBOX"
  echo "Template: $TEMPLATE"
  echo "======================================"

  ensure_sandbox

  result=$(
    docker sandbox run \
      "$SANDBOX" \
      -- "${AGENT_ARGS[@]}" "$PROMPT" \
      2>&1 | tee /dev/tty
  )

  echo ""
  echo "======================================"
  echo "Iteration $i completed"
  echo "======================================"

  if echo "$result" | grep -qEx '[[:space:]]*<promise>COMPLETE</promise>[[:space:]]*'; then
    echo "PRD complete, exiting."
    exit 0
  fi
done
