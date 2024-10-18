#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 --mysql_host <host> --mysql_port <port> --mysql_user <user> --mysql_password <password>"
    exit 1
}

# Parse input parameters
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --mysql_host)
        MYSQL_HOST="$2"
        shift # past argument
        shift # past value
        ;;
        --mysql_port)
        MYSQL_PORT="$2"
        shift
        shift
        ;;
        --mysql_user)
        MYSQL_USER="$2"
        shift
        shift
        ;;
        --mysql_password)
        MYSQL_PASSWORD="$2"
        shift
        shift
        ;;
        *)
        echo "Unknown parameter: $1"
        usage
        ;;
    esac
done

# Check if all parameters are set
if [[ -z "$MYSQL_HOST" || -z "$MYSQL_PORT" || -z "$MYSQL_USER" || -z "$MYSQL_PASSWORD" ]]; then
    echo "Error: All parameters are required."
    usage
fi

# Step 1: Check if mysqlsh exists, if not, call install_mysql_shell.sh
if ! command -v mysqlsh &> /dev/null; then
    echo "mysqlsh not found, attempting to install..."
    bash install_mysql_shell.sh
    if [[ $? -ne 0 ]]; then
        echo "Failed to install MySQL Shell. Exiting."
        exit 1
    fi
else
    echo "mysqlsh is already installed."
fi

# Step 2: Call start_snapshot.sh with MySQL parameters
echo "Starting snapshot..."
source start_snapshot.sh
if [[ $? -ne 0 ]]; then
    echo "Failed to start snapshot. Exiting."
    exit 1
fi

# Step 3: Call start_delta.sh with MySQL parameters
echo "Starting delta..."
source start_delta.sh
if [[ $? -ne 0 ]]; then
    echo "Failed to start delta. Exiting."
    exit 1
fi

echo "All steps completed successfully."