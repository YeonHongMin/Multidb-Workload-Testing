-- ============================================================================
-- PostgreSQL DDL - 부하 테스트용 스키마 (PostgreSQL 11+)
-- ============================================================================
-- 실행 방법: psql -U username -d database -f postgresql_schema.sql

-- 기존 객체 삭제 (필요시 주석 해제)
-- DROP TABLE IF EXISTS load_test CASCADE;

-- ============================================================================
-- 1. HASH PARTITION 테이블 생성 (PostgreSQL 11+)
-- ============================================================================
CREATE TABLE load_test (
    id           BIGSERIAL       PRIMARY KEY,
    thread_id    VARCHAR(50)     NOT NULL,
    value_col    VARCHAR(200),
    random_data  VARCHAR(1000),
    status       VARCHAR(20)     DEFAULT 'ACTIVE',
    created_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (id);

-- ============================================================================
-- 2. 파티션 생성 (16개)
-- ============================================================================
CREATE TABLE load_test_p00 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE load_test_p01 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE load_test_p02 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE load_test_p03 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE load_test_p04 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE load_test_p05 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE load_test_p06 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE load_test_p07 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE load_test_p08 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE load_test_p09 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE load_test_p10 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE load_test_p11 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE load_test_p12 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE load_test_p13 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE load_test_p14 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE load_test_p15 PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER 15);

-- ============================================================================
-- 3. 인덱스 생성
-- ============================================================================
CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at);
CREATE INDEX idx_load_test_created ON load_test(created_at);

-- ============================================================================
-- 4. 통계 수집 (선택사항)
-- ============================================================================
-- ANALYZE load_test;

-- ============================================================================
-- 5. 확인 쿼리
-- ============================================================================
-- 테이블 확인
SELECT 
    schemaname, 
    tablename, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename = 'load_test';

-- 파티션 테이블 확인
SELECT
    schemaname,
    tablename,
    partitiontype,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_partitioned_table pt
JOIN pg_class c ON pt.partrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_tables t ON t.tablename = c.relname
WHERE tablename = 'load_test';

-- 파티션 목록 확인
SELECT
    parent.relname AS parent_table,
    child.relname AS partition_name,
    pg_size_pretty(pg_total_relation_size(child.oid)) AS partition_size
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'load_test'
ORDER BY child.relname;

-- 인덱스 확인
SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename LIKE 'load_test%'
ORDER BY indexname;

-- SEQUENCE 확인
SELECT
    sequence_name,
    start_value,
    minimum_value,
    maximum_value,
    increment,
    cycle_option
FROM information_schema.sequences
WHERE sequence_name LIKE 'load_test%';

\echo ''
\echo '============================================================================'
\echo 'PostgreSQL schema creation completed successfully!'
\echo '============================================================================'
\echo ''
