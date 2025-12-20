#!/bin/bash
# IBM DB2 JDBC Load Test Script

# Configuration
DB_TYPE="db2"
HOST="localhost"
PORT=50000
DATABASE="testdb"
USER="db2inst1"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200
MODE="full"  # full, insert-only, select-only
JRE_DIR="./jre"

# Check JDBC driver
echo "Checking DB2 JDBC driver..."
if ! ls ./jre/db2/*jcc*.jar 1> /dev/null 2>&1; then
    echo "ERROR: DB2 JDBC driver not found in ./jre/db2/"
    echo "Place DB2 JDBC driver JAR under ./jre/db2/ (e.g. db2jcc4.jar)"
    exit 1
fi

echo "Starting DB2 JDBC load test (mode: ${MODE})..."

python multi_db_load_tester_jdbc.py \
    --db-type ${DB_TYPE} \
    --host ${HOST} \
    --port ${PORT} \
    --database ${DATABASE} \
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
