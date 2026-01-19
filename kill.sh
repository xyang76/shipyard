#!/bin/bash

# JSON of IP:port mappings
declare -A ports=(
    ["0"]="127.0.0.1:7070 127.0.0.1:7071 127.0.0.1:7072 127.0.0.1:7087"
    ["1"]="127.0.0.1:7073 127.0.0.1:7074 127.0.0.1:7075 127.0.0.1:7076"
    ["2"]="127.0.0.1:6060 127.0.0.1:32427 127.0.0.1:32437 127.0.0.1:32447"
    ["3"]="127.0.0.1:32517 127.0.0.1:32527 127.0.0.1:32537 127.0.0.1:32547"
    ["2147483647"]="127.0.0.1:38800"
)

# Function to stop the process using a specific IP:port
stop_process_using_ip_port() {
    local ip_port="$1"
    local port="${ip_port##*:}"  # extract port number after colon
    local pid

    pid=$(fuser -n tcp "$port" 2>/dev/null)

    if [ -n "$pid" ]; then
        echo "Stopping process $pid using $ip_port..."
        kill -9 "$pid" & wait "$!"
    else
        echo "No process found using $ip_port."
    fi
}


# Loop through all keys and their IP:ports
for key in "${!ports[@]}"; do
    for ip_port in ${ports[$key]}; do
        stop_process_using_ip_port "$ip_port"
    done
done

wait
echo "All processes stopped."
