#!/bin/bash

# Get the ID and index passed as arguments
id="$1"
index="$2"
sleep_time=5

num_iterations=1000
pid=10000

# Function to generate a random number between 0 and 99
generate_random() {
    echo "$(( RANDOM % 100 ))"
}

# Function to stop the process using a specific port
stop_process_using_port() {
    local port="$1"
    local pid=$(fuser -n tcp "$port" 2>/dev/null)

    if [ -n "$pid" ]; then
        echo "Stopping process $pid using port $port..."
        kill -9 "$pid" & wait $!
    else
        echo "No process found using port $port."
    fi
}

# Start the server
go run main.go -r 2 -sport 7070 -maddr 20.0.188.41 -a "$id" -s 5 -tt 10 & pid=$!

# Main loop
for (( i = 1; i <= num_iterations; i++ )); do
    sleep "$sleep_time"
    random_value=$(generate_random)
    echo "Random value is $random_value"

    # If random value < 5, simulate server failure
    if [ "$random_value" -lt 5 ]; then
        echo "Random value $random_value is less than 5, server fail."
        stop_process_using_port 7070
        stop_process_using_port 9070
        kill -9 $pid & wait $!
        sleep 2
        go run main.go -r 2 -sport 7070 -maddr 20.0.188.41 -a "$id" -s 5 -tt 10 & pid=$!
    fi
done

# Wait for server process to finish
wait

