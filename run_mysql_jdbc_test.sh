#!/bin/bash
# MySQL JDBC Load Test Script

# Configuration
DB_TYPE="mysql"
HOST="localhost"
PORT=3306
DATABASE="testdb"
USER="root"
PASSWORD="your_password"
THREAD_COUNT=100
TEST_DURATION=300
MIN_POOL_SIZE=30
MAX_POOL_SIZE=32  # MySQL pool limit
MODE="full"  # full, insert-only, select-only
JRE_DIR="./jre"

# Check JDBC driver
echo "Checking MySQL JDBC driver..."
if ! ls ./jre/mysql/mysql-connector-*.jar 1> /dev/null 2>&1; then
    echo "ERROR: MySQL JDBC driver not found in ./jre/mysql/"
    echo "Download from: https://dev.mysql.com/downloads/connector/j/"
    exit 1
fi

echo "Starting MySQL JDBC load test (mode: ${MODE})..."

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
    --log-level INFO

echo "Test completed. Check multi_db_load_test_jdbc.log for details."
