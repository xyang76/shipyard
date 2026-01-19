#!/bin/bash

# Get the ID passed as an argument
num_iterations=1000
sleep_time=30
method="$1"
# Array to store PIDs
pids=()

# Function to generate a random number between 0 and 4
generate_random() {
    echo "$(( (RANDOM % 5) + 1 ))"
}

# Function to stop the process using a specific port
stop_process_using_port() {
    local port="$1"
    local pid=$(fuser -n tcp "$port" 2>/dev/null)

    if [ -n "$pid" ]; then
        echo "Stopping process $pid using port $port..."
        kill -9 "$pid" & wait "$!"
    else
        echo "No process found using port $port."
    fi
}

go run main.go -r 1 -N 5 &
for ((i = 1; i < 6; i++)); do 
(
./start.sh "707$i" "$method" & pids+=($!)
) &
done

for (( i = 1; i <= num_iterations; i++ )); do
    sleep "$sleep_time"
    random_index=$(generate_random)
    echo "Random value is $random_index"
    echo "kill ${pids[random_index]}"
    #stop_process_using_port 1000"$random_index"
    stop_process_using_port 707"$random_index"
    kill -9 ${pids[random_index]} &
    wait $!
    sleep 5
    # Execute command
    ./start.sh "707$random_index" "$method" & pid=$!
    pids[$random_index]=$pid
    echo "Modified PIDs of $random_index: $random_id"
    echo "${pids[@]}"
done
wait

