#!/bin/bash
# Tibero 부하 테스트 실행 스크립트

# 설정 변수
DB_TYPE="tibero"
HOST="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=your-tibero-host)(PORT=8629))(CONNECT_DATA=(SERVICE_NAME=tibero)))"
USER="test_user"
PASSWORD="your_password"
THREAD_COUNT=200
TEST_DURATION=300
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200

# 부하 테스트 실행
python multi_db_load_tester_v2.py \
    --db-type ${DB_TYPE} \
    --host "${HOST}" \
    --user ${USER} \
    --password ${PASSWORD} \
    --thread-count ${THREAD_COUNT} \
    --test-duration ${TEST_DURATION} \
    --min-pool-size ${MIN_POOL_SIZE} \
    --max-pool-size ${MAX_POOL_SIZE} \
    --log-level INFO

# 참고:
# - Tibero는 Oracle 호환 DBMS로 oracledb 드라이버 사용
# - Tibero 기본 포트: 8629
# - DSN 형식은 Oracle과 동일하게 사용 가능
# - EZConnect 형식도 지원: host:port/service_name
#   예: localhost:8629/tibero
