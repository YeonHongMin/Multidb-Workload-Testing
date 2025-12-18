#!/bin/bash
# Oracle JDBC Load Test Script

# Configuration
DB_TYPE="oracle"
HOST="localhost"
PORT=1521
SID="XEPDB1"
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200
MODE="full"  # full, insert-only, select-only
JRE_DIR="./jre"

# Check JDBC driver
echo "Checking Oracle JDBC driver..."
if ! ls ./jre/oracle/ojdbc*.jar 1> /dev/null 2>&1; then
    echo "ERROR: Oracle JDBC driver not found in ./jre/oracle/"
    echo "Download from: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
    exit 1
fi

echo "Starting Oracle JDBC load test (mode: ${MODE})..."

python multi_db_load_tester_jdbc.py \
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
    --log-level INFO

echo "Test completed. Check multi_db_load_test_jdbc.log for details."
