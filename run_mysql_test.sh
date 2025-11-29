#!/bin/bash
# MySQL 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="mysql"
HOST="localhost"
PORT=3306
DATABASE="testdb"
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MAX_POOL_SIZE=200  # MySQL은 풀당 최대 32개 제한이 있음

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
    --max-pool-size 32 \
    --log-level INFO

# 참고: MySQL은 단일 풀당 최대 32개 커넥션 제한이 있습니다.
# 더 많은 커넥션이 필요한 경우 코드를 수정하여 여러 풀을 사용해야 합니다.
