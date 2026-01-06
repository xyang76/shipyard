#!/usr/bin/env bash

# Optional argument
ARG="$1"
#  go run main.go -r 2 -sport 7070 -maddr 20.0.188.41
if [ -z "$ARG" ]; then
    # No argument provided, run original commands
    go run main.go -r 1 &
    go run main.go -r 2 -sport 7070 &
    go run main.go -r 2 -sport 7071 &
    go run main.go -r 2 -sport 7072 &
    go run main.go -r 2 -sport 7073 &
    go run main.go -r 2 -sport 7074 &
else
    # Argument provided, add -a ARG to each command
    go run main.go -r 1 -a "$ARG" &
    go run main.go -r 2 -sport 7070 -a "$ARG" &
    go run main.go -r 2 -sport 7071 -a "$ARG" &
    go run main.go -r 2 -sport 7072 -a "$ARG" &
    go run main.go -r 2 -sport 7073 -a "$ARG" &
    go run main.go -r 2 -sport 7074 -a "$ARG" &
fi

wait
