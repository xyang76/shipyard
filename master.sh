#!/usr/bin/env bash
set -e

PORT=7087
LOG=master.log

# Check if port already in use
if fuser -n tcp "$PORT" >/dev/null 2>&1; then
    echo "Port $PORT already in use. Master may already be running."
    exit 1
fi

echo "Starting master on port $PORT..."
go run main.go -r 1 > "$LOG" 2>&1 &

PID=$!
echo "Master started with PID $PID (port $PORT)"

