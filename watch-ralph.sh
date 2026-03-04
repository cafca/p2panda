#!/usr/bin/env bash
# watch-ralph.sh
# Watch agent activity inside the docker sandbox in real-time.
# Run this in a separate terminal while ralph.sh is running.
#
# Usage: ./watch-ralph.sh [--provider claude|codex] [sandbox-name]
#
# Requires: jq, docker sandbox

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./watch-ralph.sh [--provider claude|codex] [sandbox-name]

Options:
  -p, --provider  Agent provider to watch. If omitted, infer from sandbox name.
  -h, --help      Show this help text.
EOF
}

PROVIDER=""
SANDBOX=""

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
      if [[ -n "$SANDBOX" ]]; then
        echo "Unexpected extra argument: $1" >&2
        usage
        exit 1
      fi
      SANDBOX="$1"
      shift
      ;;
  esac
done

if [[ -z "$PROVIDER" && -n "$SANDBOX" ]]; then
  case "$SANDBOX" in
    codex-*) PROVIDER="codex" ;;
    *) PROVIDER="claude" ;;
  esac
fi

PROVIDER="${PROVIDER:-claude}"
SANDBOX="${SANDBOX:-${PROVIDER}-p2panda}"

PROJECT_DIR="-Users-pv-code-p2panda"

case "$PROVIDER" in
  claude)
    LOG_ROOT="/home/agent/.claude/projects/$PROJECT_DIR"
    WAIT_MESSAGE="Waiting for Claude log file..."
    FIND_LOGFILE_CMD="ls -t $LOG_ROOT/*.jsonl 2>/dev/null | head -1"
    JQ_FILTER='
      def trunc(n): if (. | length) > n then .[0:n] + "…" else . end;
      def clean: gsub("\r"; "") | gsub("\t"; "  ");

      if .type == "assistant" then
        .message.content[]? |
        if .type == "text" and (.text | length) > 0 then
          "\u001b[36m💬 \(.text | clean | trunc(400))\u001b[0m"
        elif .type == "tool_use" then
          if .name == "Bash" then
            "\u001b[1m\u001b[33m⚡ Bash\u001b[0m  \u001b[37m\(.input.command | clean | trunc(200))\u001b[0m"
          elif (.name == "Edit" or .name == "MultiEdit") then
            "\u001b[1m\u001b[32m✏️  \(.name)\u001b[0m  \u001b[37m\(.input.file_path // .input.path // "")\u001b[0m"
          elif .name == "Write" then
            "\u001b[1m\u001b[32m📝 Write\u001b[0m  \u001b[37m\(.input.file_path // .input.path // "")\u001b[0m"
          elif .name == "Read" then
            "\u001b[34m📖 Read\u001b[0m   \u001b[37m\(.input.file_path // .input.path // "")\u001b[0m"
          elif (.name == "Glob" or .name == "LS") then
            "\u001b[34m🔍 \(.name)\u001b[0m    \u001b[37m\(.input | tostring | trunc(120))\u001b[0m"
          elif .name == "Grep" then
            "\u001b[34m🔎 Grep\u001b[0m   \u001b[37m\(.input.pattern // "" | trunc(120))\u001b[0m"
          else
            "\u001b[35m🔧 \(.name)\u001b[0m   \u001b[37m\(.input | tostring | trunc(150))\u001b[0m"
          end
        else empty
        end
      elif .type == "user" and .toolUseResult then
        (.toolUseResult.stdout // "") as $out |
        (.toolUseResult.stderr // "") as $err |
        (if ($out | length) > 0 then
          "   \u001b[2m↳ \($out | clean | trunc(300))\u001b[0m"
        else empty end),
        (if ($err | length) > 0 then
          "   \u001b[2m\u001b[31m↳ stderr: \($err | clean | trunc(200))\u001b[0m"
        else empty end)
      elif .type == "system" and .subtype == "init" then
        "\n\u001b[1m\u001b[36m━━━ New session: \(.sessionId // "") ━━━\u001b[0m\n"
      elif .type == "result" then
        "\n\u001b[1m\u001b[" + (if .subtype == "success" then "32" else "31" end) +
        "m━━━ \(.subtype | ascii_upcase) ━━━\u001b[0m\n"
      else empty
      end
    '
    ;;
  codex)
    LOG_ROOT="/home/agent/.codex/sessions"
    WAIT_MESSAGE="Waiting for Codex session log..."
    FIND_LOGFILE_CMD="find $LOG_ROOT -type f -name '*.jsonl' -exec ls -t {} + 2>/dev/null | head -1"
    JQ_FILTER='
      def trunc(n): if (. | length) > n then .[0:n] + "…" else . end;
      def clean: gsub("\r"; "") | gsub("\t"; "  ");

      if .type == "event_msg" and .payload.type == "agent_message" then
        "\u001b[36m💬 \(.payload.message | clean | trunc(400))\u001b[0m"
      elif .type == "event_msg" and .payload.type == "agent_reasoning" then
        "\u001b[2m🧠 \(.payload.text | clean | trunc(200))\u001b[0m"
      elif .type == "response_item" and .payload.type == "function_call" then
        "\u001b[1m\u001b[33m⚡ \(.payload.name)\u001b[0m  \u001b[37m\(.payload.arguments | clean | trunc(220))\u001b[0m"
      elif .type == "response_item" and .payload.type == "function_call_output" then
        "   \u001b[2m↳ \(.payload.output | clean | trunc(300))\u001b[0m"
      elif .type == "event_msg" and .payload.type == "token_count" then
        "\u001b[2mTokens: \(.payload.info.total_token_usage.total_tokens // 0)\u001b[0m"
      else empty
      end
    '
    ;;
  *)
    echo "Unsupported provider: $PROVIDER" >&2
    usage
    exit 1
    ;;
esac

# Colors
RESET='\033[0m'
BOLD='\033[1m'
CYAN='\033[36m'
GREEN='\033[32m'
DIM='\033[2m'

echo -e "${CYAN}Waiting for sandbox '${SANDBOX}' (${PROVIDER})...${RESET}"
until docker sandbox exec "$SANDBOX" bash -c "test -d '$LOG_ROOT'" 2>/dev/null; do
  sleep 1
done

echo -e "${CYAN}${WAIT_MESSAGE}${RESET}"
LOGFILE=""
until [[ -n "$LOGFILE" ]]; do
  LOGFILE=$(docker sandbox exec "$SANDBOX" bash -lc "$FIND_LOGFILE_CMD")
  sleep 1
done

echo -e "${GREEN}${BOLD}Watching:${RESET} ${LOGFILE}"
echo -e "${DIM}────────────────────────────────────────────────────────────${RESET}"

docker sandbox exec "$SANDBOX" bash -lc "tail -n +1 -f '$LOGFILE'" | \
jq -r --unbuffered "$JQ_FILTER" 2>/dev/null
