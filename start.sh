#!/usr/bin/env bash
pwd

port="$1"
method="$2"
recovered="$3"

go run main.go -r 2 -sport "$port" -a "$method" -rec "$recovered"


