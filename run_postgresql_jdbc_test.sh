#!/bin/bash
# PostgreSQL JDBC 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="postgresql"
HOST="localhost"
PORT=5432
DATABASE="testdb"
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=200
MAX_POOL_SIZE=300
JRE_DIR="./jre"

# JDBC 드라이버 확인
echo "Checking PostgreSQL JDBC driver..."
if ! ls ./jre/postgresql/postgresql-*.jar 1> /dev/null 2>&1; then
    echo "ERROR: PostgreSQL JDBC driver not found in ./jre/postgresql/"
    echo "Please download from: https://jdbc.postgresql.org/download/"
    exit 1
fi

echo "Starting PostgreSQL JDBC load test..."

# 부하 테스트 실행
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
    --jre-dir ${JRE_DIR} \
    --log-level INFO

echo "Test completed. Check multi_db_load_test_jdbc.log for details."
