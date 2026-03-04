# ralph.sh
# Usage: ./ralph.sh <iterations>

set -ex
set -o pipefail

SANDBOX="claude-p2panda"
TEMPLATE="claude-p2panda-template:latest"

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

if [ -z "$1" ]; then
  echo "Usage: $0 <iterations>"
  exit 1
fi

for ((i=1; i<=$1; i++)); do
  echo "======================================"
  echo "Starting iteration $i of $1"
  echo "======================================"

  # Use template only when creating a new sandbox; reuse existing one as-is.
  if docker sandbox ls | grep -q "^${SANDBOX} "; then
    RUN_CMD="docker sandbox run ${SANDBOX}"
  else
    RUN_CMD="docker sandbox run -t ${TEMPLATE} --name ${SANDBOX} claude"
  fi

  result=$(${RUN_CMD} -- --print --verbose "$PROMPT" 2>&1 | tee /dev/tty)

  echo ""
  echo "======================================"
  echo "Iteration $i completed"
  echo "======================================"

  if echo "$result" | grep -qF '<promise>COMPLETE</promise>'; then
    echo "PRD complete, exiting."
    exit 0
  fi
done
