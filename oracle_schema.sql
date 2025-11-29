-- ============================================================================
-- Oracle DDL - 부하 테스트용 스키마
-- ============================================================================
-- 실행 방법: sqlplus username/password@service_name @oracle_schema.sql

-- 기존 객체 삭제 (필요시 주석 해제)
-- DROP TABLE LOAD_TEST PURGE;
-- DROP SEQUENCE LOAD_TEST_SEQ;

-- ============================================================================
-- 1. SEQUENCE 생성
-- ============================================================================
CREATE SEQUENCE LOAD_TEST_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 1000          -- 성능 향상을 위해 캐시 사용
    NOCYCLE
    ORDER;

-- ============================================================================
-- 2. HASH PARTITION 테이블 생성 (16개 파티션)
-- ============================================================================
CREATE TABLE LOAD_TEST (
    ID           NUMBER(19)      NOT NULL,
    THREAD_ID    VARCHAR2(50)    NOT NULL,
    VALUE_COL    VARCHAR2(200),
    RANDOM_DATA  VARCHAR2(1000),
    STATUS       VARCHAR2(20)    DEFAULT 'ACTIVE',
    CREATED_AT   TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_AT   TIMESTAMP       DEFAULT SYSTIMESTAMP
)
PARTITION BY HASH (ID)
(
    PARTITION P01, PARTITION P02, PARTITION P03, PARTITION P04,
    PARTITION P05, PARTITION P06, PARTITION P07, PARTITION P08,
    PARTITION P09, PARTITION P10, PARTITION P11, PARTITION P12,
    PARTITION P13, PARTITION P14, PARTITION P15, PARTITION P16
)
TABLESPACE USERS        -- 실제 환경에 맞게 변경
ENABLE ROW MOVEMENT;

-- ============================================================================
-- 3. PRIMARY KEY 제약조건
-- ============================================================================
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

-- ============================================================================
-- 4. 인덱스 생성
-- ============================================================================
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL;

-- ============================================================================
-- 5. 통계 수집 (선택사항)
-- ============================================================================
-- EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'LOAD_TEST', CASCADE => TRUE);

-- ============================================================================
-- 6. 확인 쿼리
-- ============================================================================
-- 테이블 확인
SELECT TABLE_NAME, PARTITIONED, NUM_ROWS FROM USER_TABLES 
WHERE TABLE_NAME = 'LOAD_TEST';

-- 파티션 목록
SELECT PARTITION_NAME, PARTITION_POSITION, HIGH_VALUE 
FROM USER_TAB_PARTITIONS 
WHERE TABLE_NAME = 'LOAD_TEST'
ORDER BY PARTITION_POSITION;

-- 인덱스 확인
SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS, STATUS 
FROM USER_INDEXES 
WHERE TABLE_NAME = 'LOAD_TEST';

-- SEQUENCE 확인
SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, CACHE_SIZE, LAST_NUMBER
FROM USER_SEQUENCES
WHERE SEQUENCE_NAME = 'LOAD_TEST_SEQ';

PROMPT
PROMPT ============================================================================
PROMPT Oracle schema creation completed successfully!
PROMPT ============================================================================
PROMPT
