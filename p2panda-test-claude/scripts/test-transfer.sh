#!/usr/bin/env bash

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Building p2panda-file-sharing...${NC}"
cargo build --release -p p2panda-file-sharing

# Create temporary directory for test
TEMP_DIR=$(mktemp -d)
echo -e "${YELLOW}Using temp directory: $TEMP_DIR${NC}"

# Create sample file
SAMPLE_FILE="$TEMP_DIR/sample.txt"
echo "Hello from p2panda!" > "$SAMPLE_FILE"
echo -e "${YELLOW}Created sample file: $SAMPLE_FILE${NC}"

# Create output directory for receiver
OUTPUT_DIR="$TEMP_DIR/received"
mkdir -p "$OUTPUT_DIR"

# Binary path and shared gossip topic
BINARY="./target/release/p2panda-file-sharing"
TOPIC="test-transfer"

# Start receiver in background
echo -e "${YELLOW}Starting receiver...${NC}"
RUST_LOG=info "$BINARY" receive --topic "$TOPIC" --output-dir "$OUTPUT_DIR" > "$TEMP_DIR/receiver.log" 2>&1 &
RECEIVER_PID=$!

# Give receiver time to start and bind mDNS
echo -e "${YELLOW}Waiting for receiver to initialize (2 seconds)...${NC}"
sleep 2

# Check if receiver is still running
if ! kill -0 "$RECEIVER_PID" 2>/dev/null; then
    echo -e "${RED}FAIL: Receiver process died${NC}"
    cat "$TEMP_DIR/receiver.log"
    rm -rf "$TEMP_DIR"
    exit 1
fi

# Start sender in background
echo -e "${YELLOW}Starting sender...${NC}"
RUST_LOG=info "$BINARY" send --topic "$TOPIC" "$SAMPLE_FILE" > "$TEMP_DIR/sender.log" 2>&1 &
SENDER_PID=$!

# Wait for file to appear (poll for up to 30 seconds)
echo -e "${YELLOW}Waiting for file transfer (up to 30 seconds)...${NC}"
TIMEOUT=30
ELAPSED=0
SUCCESS=0

while [ $ELAPSED -lt $TIMEOUT ]; do
    if [ -f "$OUTPUT_DIR/sample.txt" ]; then
        SUCCESS=1
        break
    fi
    sleep 1
    ELAPSED=$((ELAPSED + 1))
done

# Kill both processes
echo -e "${YELLOW}Stopping processes...${NC}"
kill $RECEIVER_PID 2>/dev/null || true
kill $SENDER_PID 2>/dev/null || true

# Wait for processes to exit
sleep 1

# Check if file was received
if [ $SUCCESS -eq 1 ]; then
    # Compare content
    if diff -q "$SAMPLE_FILE" "$OUTPUT_DIR/sample.txt" > /dev/null; then
        echo -e "${GREEN}PASS: File transferred successfully!${NC}"
        echo -e "${GREEN}Content verified.${NC}"

        # Show logs
        echo -e "\n${YELLOW}Sender log:${NC}"
        cat "$TEMP_DIR/sender.log" | grep -v "DEBUG" | tail -n 10
        echo -e "\n${YELLOW}Receiver log:${NC}"
        cat "$TEMP_DIR/receiver.log" | grep -v "DEBUG" | tail -n 10

        # Cleanup
        rm -rf "$TEMP_DIR"
        exit 0
    else
        echo -e "${RED}FAIL: File content mismatch${NC}"
        echo "Expected:"
        cat "$SAMPLE_FILE"
        echo "Received:"
        cat "$OUTPUT_DIR/sample.txt"
        rm -rf "$TEMP_DIR"
        exit 1
    fi
else
    echo -e "${RED}FAIL: File not received within $TIMEOUT seconds${NC}"
    echo -e "\n${YELLOW}Sender log:${NC}"
    cat "$TEMP_DIR/sender.log"
    echo -e "\n${YELLOW}Receiver log:${NC}"
    cat "$TEMP_DIR/receiver.log"
    rm -rf "$TEMP_DIR"
    exit 1
fi
