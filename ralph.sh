#!/usr/bin/env bash
# ralph.sh
# Usage: ./ralph.sh [--provider claude|codex] [--sandbox name] [--template image] <iterations>

set -euo pipefail

PROMPT='@PRD.jsonl @progress.txt
1. Decide which task to work on next.
This should be the one YOU decide has the highest priority,
- not necessarily the first in the list.
2. Check any feedback loops, such as types and tests.
3. Append your progress to the progress.txt file.
4. Update the PRD.jsonl file and make sure to ONLY change the `passes` field of a SINGLE task. Dont make any other edits.
4. Make a git commit of that feature.
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
    AGENT_BIN="claude"
    AGENT_ARGS=(--print --verbose "$PROMPT")
    ;;
  codex)
    SANDBOX="${SANDBOX:-codex-p2panda}"
    TEMPLATE="${TEMPLATE:-codex-p2panda-template:latest}"
    AGENT_BIN="codex"
    AGENT_ARGS=(exec --color never "$PROMPT")
    ;;
  *)
    echo "Unsupported provider: $PROVIDER" >&2
    usage
    exit 1
    ;;
esac

set -x

for ((i=1; i<=ITERATIONS; i++)); do
  echo "======================================"
  echo "Starting iteration $i of $ITERATIONS with provider '$PROVIDER'"
  echo "Sandbox: $SANDBOX"
  echo "Template: $TEMPLATE"
  echo "======================================"

  # Use the template only when creating a new sandbox; reuse existing one as-is.
  if docker sandbox ls | grep -q "^${SANDBOX} "; then
    RUN_CMD=(docker sandbox run "$SANDBOX")
  else
    RUN_CMD=(docker sandbox run -t "$TEMPLATE" --name "$SANDBOX" "$AGENT_BIN")
  fi

  result=$("${RUN_CMD[@]}" -- "${AGENT_ARGS[@]}" 2>&1 | tee /dev/tty)

  echo ""
  echo "======================================"
  echo "Iteration $i completed"
  echo "======================================"

  if echo "$result" | grep -qEx '[[:space:]]*<promise>COMPLETE</promise>[[:space:]]*'; then
    echo "PRD complete, exiting."
    exit 0
  fi
done
