#!/bin/bash
# SQL Server 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="sqlserver"
HOST="localhost"  # 또는 "server,port" 형식
PORT=1433
DATABASE="testdb"
USER="sa"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200

# 부하 테스트 실행
python multi_db_load_tester_v2.py \
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
    --log-level INFO

# 참고:
# - SQL Server는 pyodbc 드라이버 사용
# - Windows에서는 "ODBC Driver 17 for SQL Server" 설치 필요
# - Linux에서는 Microsoft ODBC Driver for SQL Server 설치 필요
#   https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
