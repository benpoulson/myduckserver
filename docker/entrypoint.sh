#!/bin/bash

export DATA_PATH="${HOME}/data"
export LOG_PATH="${HOME}/log"
export REPLICA_SETUP_PATH="${HOME}/replica-setup"
export PID_FILE="${LOG_PATH}/myDuckServer.pid"

# Function to run replica setup
run_replica_setup() {
    if [ -z "$mysql_host" ] || [ -z "$mysql_port" ] || [ -z "$mysql_user" ] || [ -z "$mysql_password" ]; then
        echo "Error: Missing required MySQL connection variables for replica setup."
        exit 1
    fi
    echo "Creating replica with MySQL server at $mysql_host:$mysql_port..."
    cd "$REPLICA_SETUP_PATH" || { echo "Error: Could not change directory to ${REPLICA_SETUP_PATH}"; exit 1; }

    # Run replica_setup.sh and check for errors
    if bash replica_setup.sh --mysql_host "$mysql_host" --mysql_port "$mysql_port" --mysql_user "$mysql_user" --mysql_password "$mysql_password"; then
        echo "Replica setup completed."
    else
        echo "Error: Replica setup failed."
        exit 1
    fi
}

run_server() {
      cd "$DATA_PATH" || { echo "Error: Could not change directory to ${DATA_PATH}"; exit 1; }
      nohup myduckserver >> "${LOG_PATH}"/server.log 2>&1 &
      echo "$!" > "${PID_FILE}"
}

wait_for_my_duck_server_ready() {
    local host="127.0.0.1"
    local user="root"
    local port="3306"
    local max_attempts=30
    local attempt=0
    local wait_time=2

    echo "Waiting for MyDuckServer at $host:$port to be ready..."

    until mysqlsh --sql --host "$host" --user "$user" --password="" --port "$port" --execute "SELECT 1;" &> /dev/null; do
        attempt=$((attempt+1))
        if [ "$attempt" -ge "$max_attempts" ]; then
            echo "Error: MySQL connection timed out after $max_attempts attempts."
            exit 1
        fi
        echo "Attempt $attempt/$max_attempts: MyDuckServer is unavailable - retrying in $wait_time seconds..."
        sleep $wait_time
    done

    echo "MyDuckServer is ready!"
}


# Function to check if a process is alive by its PID file
check_process_alive() {
    local pid_file="$1"
    local proc_name="$2"

    if [[ -f "${pid_file}" ]]; then
        local pid
        pid=$(<"${pid_file}")

        if [[ -n "${pid}" && -e "/proc/${pid}" ]]; then
            return 0  # Process is running
        else
            echo "${proc_name} (PID: ${pid}) is not running."
            return 1
        fi
    else
        echo "PID file for ${proc_name} not found!"
        return 1
    fi
}

# Handle the setup_mode
setup() {
    mkdir -p "${DATA_PATH}"
    mkdir -p "${LOG_PATH}"
    case "$setup_mode" in
        "" | "server_only")
            echo "Starting MyDuckServer in server_only mode..."
            run_server
            ;;

        "replica_only")
            echo "Running in replica_only mode..."
            run_replica_setup
            ;;

        "combined")
            echo "Starting MyDuckServer and running replica setup in combined mode..."
            run_server
            wait_for_my_duck_server_ready
            run_replica_setup
            ;;

        *)
            echo "Error: Invalid setup_mode value. Valid options are: server_only, replica_only, combined."
            exit 1
            ;;
    esac
}

setup
while [[ "$setup_mode" != "replica_only" ]]; do
    # Check if the processes have started
    check_process_alive "$PID_FILE" "MyDuckServer"
    MY_DUCK_SERVER_STATUS=$?
    if (( MY_DUCK_SERVER_STATUS != 0 )); then
        echo "MyDuckServer is not running. Exiting..."
        exit 1
    fi

    # Sleep before the next status check
    sleep 10
done
