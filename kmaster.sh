#!/usr/bin/env bash

PORT=7087
PID=$(fuser -n tcp "$PORT" 2>/dev/null)

if [ -n "$PID" ]; then
    echo "Stopping master process $PID on port $PORT..."

    # Try graceful shutdown
    kill "$PID"
    sleep 1

    # Force kill if still alive
    if kill -0 "$PID" 2>/dev/null; then
        echo "Process still running, force killing..."
        kill -9 "$PID"
    fi

    echo "Master stopped."
else
    echo "No process found using port $PORT."
fi

