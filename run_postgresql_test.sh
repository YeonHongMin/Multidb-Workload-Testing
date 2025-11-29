#!/bin/bash
# PostgreSQL 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="postgresql"
HOST="localhost"
PORT=5432
DATABASE="testdb"
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200

# 부하 테스트 실행
python multi_db_load_tester.py \
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
