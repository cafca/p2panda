# ralph.sh
# Usage: ./ralph.sh <iterations>

set -ex
set -o pipefail

if [ -z "$1" ]; then
  echo "Usage: $0 <iterations>"
  exit 1
fi

# For each iteration, run Claude Code with the following prompt.
# This prompt is basic, we'll expand it later.
for ((i=1; i<=$1; i++)); do
  echo "======================================"
  echo "Starting iteration $i of $1"
  echo "======================================"

  result=$(docker sandbox run claude --debug -- -p \
"@BLOBS_PORT_REMAINING.md @progress.txt \
1. Decide which task to work on next. \
This should be the one YOU decide has the highest priority, \
- not necessarily the first in the list. \
2. Check any feedback loops, such as types and tests. \
3. Append your progress to the progress.txt file. \
4. Make a git commit of that feature. \
ONLY WORK ON A SINGLE FEATURE. \
If, while implementing the feature, you notice that all work \
is complete, output <promise>COMPLETE</promise>. \
" 2>&1 | tee /dev/tty)

  echo ""
  echo "======================================"
  echo "Iteration $i completed"
  echo "======================================"

  if [[ "$result" == *"<promise>COMPLETE</promise>"* ]]; then
    echo "PRD complete, exiting."
    exit 0
  fi
done
