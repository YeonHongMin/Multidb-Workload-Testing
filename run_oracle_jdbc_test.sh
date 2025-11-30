#!/bin/bash
# Oracle JDBC 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="oracle"
HOST="localhost"
PORT=1521
SID="XEPDB1"           # 또는 Service Name
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=200
MAX_POOL_SIZE=300
JRE_DIR="./jre"

# JDBC 드라이버 확인
echo "Checking Oracle JDBC driver..."
if [ ! -f "./jre/oracle/ojdbc8.jar" ] && [ ! -f "./jre/oracle/ojdbc11.jar" ]; then
    echo "ERROR: Oracle JDBC driver not found in ./jre/oracle/"
    echo "Please download from: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
    exit 1
fi

echo "Starting Oracle JDBC load test..."

# 부하 테스트 실행
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
    --jre-dir ${JRE_DIR} \
    --log-level INFO

echo "Test completed. Check multi_db_load_test_jdbc.log for details."
