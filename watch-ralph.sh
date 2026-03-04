#!/usr/bin/env bash
# watch-ralph.sh
# Watch Claude's activity inside the docker sandbox in real-time.
# Run this in a separate terminal while ralph.sh is running.
#
# Usage: ./watch-ralph.sh [sandbox-name]
#
# Requires: jq, docker sandbox

SANDBOX="${1:-claude-p2panda}"
PROJECT_DIR="-Users-pv-code-p2panda"
LOG_DIR="/home/agent/.claude/projects/$PROJECT_DIR"

# Colors
RESET='\033[0m'
BOLD='\033[1m'
CYAN='\033[36m'
GREEN='\033[32m'
DIM='\033[2m'

echo -e "${CYAN}Waiting for sandbox '${SANDBOX}'...${RESET}"
until docker sandbox exec "$SANDBOX" bash -c "test -d $LOG_DIR" 2>/dev/null; do
  sleep 1
done

echo -e "${CYAN}Waiting for Claude log file...${RESET}"
LOGFILE=""
until [ -n "$LOGFILE" ]; do
  LOGFILE=$(docker sandbox exec "$SANDBOX" bash -c \
    "ls -t $LOG_DIR/*.jsonl 2>/dev/null | head -1")
  sleep 1
done

echo -e "${GREEN}${BOLD}Watching:${RESET} ${LOGFILE}"
echo -e "${DIM}────────────────────────────────────────────────────────────${RESET}"

docker sandbox exec "$SANDBOX" bash -c "tail -n +1 -f '$LOGFILE'" | \
jq -r --unbuffered '
  def trunc(n): if (. | length) > n then .[0:n] + "…" else . end;

  def clean: gsub("\r"; "") | gsub("\t"; "  ");

  # ── assistant actions ──────────────────────────────────────────
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

  # ── tool results (on "user" messages that are tool responses) ──
  elif .type == "user" and .toolUseResult then
    (.toolUseResult.stdout // "") as $out |
    (.toolUseResult.stderr // "") as $err |
    (if ($out | length) > 0 then
      "   \u001b[2m↳ \($out | clean | trunc(300))\u001b[0m"
    else empty end),
    (if ($err | length) > 0 then
      "   \u001b[2m\u001b[31m↳ stderr: \($err | clean | trunc(200))\u001b[0m"
    else empty end)

  # ── session markers ────────────────────────────────────────────
  elif .type == "system" and .subtype == "init" then
    "\n\u001b[1m\u001b[36m━━━ New session: \(.sessionId // "") ━━━\u001b[0m\n"

  elif .type == "result" then
    "\n\u001b[1m\u001b[" + (if .subtype == "success" then "32" else "31" end) +
    "m━━━ \(.subtype | ascii_upcase) ━━━\u001b[0m\n"

  else empty
  end
' 2>/dev/null
