#!/bin/bash
# Oracle JDBC Load Test Script

# Configuration
DB_TYPE="oracle"
HOST="192.168.0.172"
PORT=1521
SID="DEV"
USER="app"
PASSWORD="app"
THREAD_COUNT=20
TEST_DURATION=180
MIN_POOL_SIZE=40
MAX_POOL_SIZE=80
MODE="full"  # full, insert-only, select-only
JRE_DIR="./jre"

# Virtual environment activation
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -d "$SCRIPT_DIR/.venv" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

# JAVA_HOME setup (prefer JDK 17 or 21)
if [ -z "$JAVA_HOME" ]; then
    if [ -d "/usr/lib/jvm/java-17-openjdk" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk"
    elif [ -d "/usr/lib/jvm/java-21-openjdk" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-21-openjdk"
    fi
fi

# Check JDBC driver
echo "Checking Oracle JDBC driver..."
if ! ls ./jre/oracle/ojdbc*.jar 1> /dev/null 2>&1; then
    echo "ERROR: Oracle JDBC driver not found in ./jre/oracle/"
    echo "Download from: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
    exit 1
fi

echo "Starting Oracle JDBC load test (mode: ${MODE})..."
echo "JAVA_HOME: ${JAVA_HOME:-not set}"

python3 multi_db_load_tester_jdbc.py \
    --db-type ${DB_TYPE} \
    --host ${HOST} \
    --port ${PORT} \
    --sid ${SID} \
    --user ${USER} \
    --password ${PASSWORD} \
    --thread-count ${THREAD_COUNT} \
    --test-duration ${TEST_DURATION} \
    --min-pool-size ${MIN_POOL_SIZE} \
    --max-pool-size ${MAX_POOL_SIZE} \
    --truncate \
    --mode ${MODE} \
    --jre-dir ${JRE_DIR} \
    --log-level INFO \
    --connection-timeout 5

echo "Test completed. Check multi_db_load_test_jdbc.log for details."
