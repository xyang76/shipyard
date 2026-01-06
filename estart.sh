#!/usr/bin/env bash

# Optional argument
MADDR="$1"
ARG="$2"

#  go run main.go -r 2 -sport 7070 -maddr 20.0.188.41
# ./estart.sh "20.0.188.41" 4
if [ -z "$ARG" ]; then
    # No argument provided, run original commands
    go run main.go -r 2 -sport 7070 -maddr "$MADDR" &
    go run main.go -r 2 -sport 7071 -maddr "$MADDR" &
    go run main.go -r 2 -sport 7072 -maddr "$MADDR" &
    go run main.go -r 2 -sport 7073 -maddr "$MADDR" &
    go run main.go -r 2 -sport 7074 -maddr "$MADDR" &
else
    # Argument provided, add -a ARG to each command
    go run main.go -r 2 -sport 7070 -a "$ARG" -maddr "$MADDR" &
    go run main.go -r 2 -sport 7071 -a "$ARG" -maddr "$MADDR" &
    go run main.go -r 2 -sport 7072 -a "$ARG" -maddr "$MADDR" &
    go run main.go -r 2 -sport 7073 -a "$ARG" -maddr "$MADDR" &
    go run main.go -r 2 -sport 7074 -a "$ARG" -maddr "$MADDR" &
fi

wait
