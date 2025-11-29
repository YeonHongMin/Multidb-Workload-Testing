#!/bin/bash
# Oracle 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="oracle"
HOST="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=your-oracle-host)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))"
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200

# 부하 테스트 실행
python multi_db_load_tester.py \
    --db-type ${DB_TYPE} \
    --host "${HOST}" \
    --user ${USER} \
    --password ${PASSWORD} \
    --thread-count ${THREAD_COUNT} \
    --test-duration ${TEST_DURATION} \
    --min-pool-size ${MIN_POOL_SIZE} \
    --max-pool-size ${MAX_POOL_SIZE} \
    --log-level INFO
