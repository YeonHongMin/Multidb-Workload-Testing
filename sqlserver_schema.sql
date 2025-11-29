-- ============================================================================
-- SQL Server DDL - 부하 테스트용 스키마 (SQL Server 2016+)
-- ============================================================================
-- 실행 방법: sqlcmd -S server -d database -U user -P password -i sqlserver_schema.sql

USE [your_database_name];  -- 실제 데이터베이스 이름으로 변경
GO

-- 기존 객체 삭제 (필요시 주석 해제)
/*
DROP TABLE IF EXISTS load_test;
DROP PARTITION SCHEME PS_LoadTest;
DROP PARTITION FUNCTION PF_LoadTest;
GO
*/

-- ============================================================================
-- 1. 파티션 함수 생성 (16개 파티션)
-- ============================================================================
-- HASH 파티션을 흉내내기 위한 RANGE 파티션 사용
CREATE PARTITION FUNCTION PF_LoadTest (BIGINT)
AS RANGE LEFT FOR VALUES (
    625000000,    -- Partition 1
    1250000000,   -- Partition 2
    1875000000,   -- Partition 3
    2500000000,   -- Partition 4
    3125000000,   -- Partition 5
    3750000000,   -- Partition 6
    4375000000,   -- Partition 7
    5000000000,   -- Partition 8
    5625000000,   -- Partition 9
    6250000000,   -- Partition 10
    6875000000,   -- Partition 11
    7500000000,   -- Partition 12
    8125000000,   -- Partition 13
    8750000000,   -- Partition 14
    9375000000    -- Partition 15
);
GO

-- ============================================================================
-- 2. 파티션 스키마 생성
-- ============================================================================
-- 모든 파티션을 PRIMARY 파일그룹에 배치
-- 프로덕션 환경에서는 여러 파일그룹으로 분산 권장
CREATE PARTITION SCHEME PS_LoadTest
AS PARTITION PF_LoadTest
ALL TO ([PRIMARY]);
GO

-- ============================================================================
-- 3. 테이블 생성
-- ============================================================================
CREATE TABLE load_test (
    id           BIGINT         IDENTITY(1,1) NOT NULL,
    thread_id    NVARCHAR(50)   NOT NULL,
    value_col    NVARCHAR(200),
    random_data  NVARCHAR(1000),
    status       NVARCHAR(20)   DEFAULT 'ACTIVE',
    created_at   DATETIME2      DEFAULT GETDATE(),
    updated_at   DATETIME2      DEFAULT GETDATE(),
    CONSTRAINT PK_load_test PRIMARY KEY CLUSTERED (id)
) ON PS_LoadTest(id);
GO

-- ============================================================================
-- 4. 인덱스 생성
-- ============================================================================
CREATE NONCLUSTERED INDEX idx_load_test_thread 
ON load_test(thread_id, created_at)
ON PS_LoadTest(id);
GO

CREATE NONCLUSTERED INDEX idx_load_test_created 
ON load_test(created_at)
ON PS_LoadTest(id);
GO

-- ============================================================================
-- 5. 확인 쿼리
-- ============================================================================
-- 테이블 정보 확인
SELECT 
    t.name AS TableName,
    ps.name AS PartitionScheme,
    pf.name AS PartitionFunction,
    pf.fanout AS PartitionCount
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
WHERE t.name = 'load_test';
GO

-- 파티션별 행 수 확인
SELECT 
    t.name AS TableName,
    i.name AS IndexName,
    p.partition_number AS PartitionNumber,
    p.rows AS RowCount,
    fg.name AS FileGroup
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units au ON p.partition_id = au.container_id
JOIN sys.filegroups fg ON au.data_space_id = fg.data_space_id
WHERE t.name = 'load_test' AND i.index_id IN (0, 1)
ORDER BY p.partition_number;
GO

-- 인덱스 목록 확인
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_primary_key AS IsPrimaryKey,
    COL_NAME(ic.object_id, ic.column_id) AS ColumnName
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE i.object_id = OBJECT_ID('load_test')
ORDER BY i.name, ic.key_ordinal;
GO

PRINT '';
PRINT '============================================================================';
PRINT 'SQL Server schema creation completed successfully!';
PRINT '============================================================================';
PRINT '';
GO
