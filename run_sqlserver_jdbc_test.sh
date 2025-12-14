#!/bin/bash
# SQL Server JDBC Load Test Script

# Configuration
DB_TYPE="sqlserver"
HOST="localhost"
PORT=1433
DATABASE="testdb"
USER="sa"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200
MODE="full"  # full, insert-only, select-only
JRE_DIR="./jre"

# Check JDBC driver
echo "Checking SQL Server JDBC driver..."
if ! ls ./jre/sqlserver/mssql-jdbc-*.jar 1> /dev/null 2>&1; then
    echo "ERROR: SQL Server JDBC driver not found in ./jre/sqlserver/"
    echo "Download from: https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server"
    exit 1
fi

echo "Starting SQL Server JDBC load test (mode: ${MODE})..."

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
    --mode ${MODE} \
    --jre-dir ${JRE_DIR} \
    --log-level INFO

echo "Test completed. Check multi_db_load_test_jdbc.log for details."
