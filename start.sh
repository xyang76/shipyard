#!/usr/bin/env bash
pwd

port="$1"
method="$2"

go run main.go -r 2 -sport "$port" -a "$method"


