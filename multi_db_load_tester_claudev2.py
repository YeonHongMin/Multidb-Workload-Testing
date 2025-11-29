#!/usr/bin/env python3
"""
멀티 데이터베이스 부하 테스트 프로그램
Oracle, PostgreSQL, MySQL, SQL Server, Tibero 지원

특징:
- 5개 데이터베이스 타입 지원 (Oracle, PostgreSQL, MySQL, SQL Server, Tibero)
- 멀티스레드 + 커넥션 풀링
- INSERT -> COMMIT -> SELECT 검증 패턴
- 자동 에러 복구 및 커넥션 재연결
- 실시간 성능 모니터링 (TPS, 에러 카운트)
- 100~1000개 동시 세션 지원
"""

import sys
import time
import logging
import threading
import argparse
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Protocol
from dataclasses import dataclass
from abc import ABC, abstractmethod
import queue

# 데이터베이스 드라이버 임포트 (조건부)
try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

try:
    import psycopg2
    from psycopg2 import pool as pg_pool
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import mysql.connector
    from mysql.connector import pooling as mysql_pooling
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import pyodbc
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False

# Tibero는 Oracle 드라이버와 호환
TIBERO_AVAILABLE = ORACLE_AVAILABLE

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)-15s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_db_load_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# 성능 카운터 (Thread-Safe)
# ============================================================================
class PerformanceCounter:
    """스레드 안전 성능 카운터"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.total_inserts = 0
        self.total_selects = 0
        self.total_errors = 0
        self.verification_failures = 0
        self.connection_recreates = 0
        self.start_time = time.time()
        
    def increment_insert(self):
        with self.lock:
            self.total_inserts += 1
    
    def increment_select(self):
        with self.lock:
            self.total_selects += 1
    
    def increment_error(self):
        with self.lock:
            self.total_errors += 1
    
    def increment_verification_failure(self):
        with self.lock:
            self.verification_failures += 1
    
    def increment_connection_recreate(self):
        with self.lock:
            self.connection_recreates += 1
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            elapsed_time = time.time() - self.start_time
            tps = self.total_inserts / elapsed_time if elapsed_time > 0 else 0
            return {
                'total_inserts': self.total_inserts,
                'total_selects': self.total_selects,
                'total_errors': self.total_errors,
                'verification_failures': self.verification_failures,
                'connection_recreates': self.connection_recreates,
                'elapsed_seconds': elapsed_time,
                'tps': round(tps, 2)
            }


# 전역 성능 카운터
perf_counter = PerformanceCounter()


# ============================================================================
# 데이터베이스 어댑터 인터페이스
# ============================================================================
class DatabaseAdapter(ABC):
    """데이터베이스 공통 인터페이스"""
    
    @abstractmethod
    def create_connection_pool(self, config: 'DatabaseConfig'):
        """커넥션 풀 생성"""
        pass
    
    @abstractmethod
    def get_connection(self):
        """커넥션 획득"""
        pass
    
    @abstractmethod
    def release_connection(self, connection, is_error: bool = False):
        """커넥션 반환"""
        pass
    
    @abstractmethod
    def close_pool(self):
        """풀 종료"""
        pass
    
    @abstractmethod
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """INSERT 실행 후 생성된 ID 반환"""
        pass
    
    @abstractmethod
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """SELECT 실행"""
        pass
    
    @abstractmethod
    def commit(self, connection):
        """트랜잭션 커밋"""
        pass
    
    @abstractmethod
    def rollback(self, connection):
        """트랜잭션 롤백"""
        pass
    
    @abstractmethod
    def get_ddl(self) -> str:
        """DDL 스크립트 반환"""
        pass


# ============================================================================
# Oracle 어댑터
# ============================================================================
class OracleAdapter(DatabaseAdapter):
    """Oracle 데이터베이스 어댑터"""
    
    def __init__(self):
        if not ORACLE_AVAILABLE:
            raise ImportError("oracledb module not available. Install with: pip install oracledb")
        self.pool = None
        
    def create_connection_pool(self, config: 'DatabaseConfig'):
        logger.info(f"Creating Oracle session pool (min={config.min_pool_size}, max={config.max_pool_size})")
        
        self.pool = oracledb.create_pool(
            user=config.user,
            password=config.password,
            dsn=config.host,
            min=config.min_pool_size,
            max=config.max_pool_size,
            increment=config.pool_increment,
            getmode=oracledb.POOL_GETMODE_WAIT,
            threaded=True
        )
        
        logger.info(f"Oracle session pool created. Current size: {self.pool.opened}")
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing Oracle connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """Oracle INSERT with SEQUENCE"""
        sql = """
        INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
        VALUES (LOAD_TEST_SEQ.NEXTVAL, :thread_id, :value_col, :random_data, SYSTIMESTAMP)
        RETURNING ID INTO :new_id
        """
        
        new_id_var = cursor.var(int)
        cursor.execute(sql, {
            'thread_id': thread_id,
            'value_col': f'TEST_{thread_id}',
            'random_data': random_data,
            'new_id': new_id_var
        })
        
        return new_id_var.getvalue()[0]
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        sql = "SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = :id"
        cursor.execute(sql, {'id': record_id})
        return cursor.fetchone()
    
    def commit(self, connection):
        connection.commit()
    
    def rollback(self, connection):
        try:
            connection.rollback()
        except:
            pass
    
    def get_ddl(self) -> str:
        return """
-- ============================================================================
-- Oracle DDL
-- ============================================================================

-- 1. SEQUENCE 생성
CREATE SEQUENCE LOAD_TEST_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 1000
    NOCYCLE
    ORDER;

-- 2. HASH PARTITION 테이블 생성 (16개 파티션)
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
TABLESPACE USERS
ENABLE ROW MOVEMENT;

-- 3. PRIMARY KEY 제약조건
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

-- 4. 인덱스 생성
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL;

-- 5. 통계 수집 (선택사항)
-- EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'LOAD_TEST', CASCADE => TRUE);

-- 6. 테이블 확인
SELECT TABLE_NAME, PARTITIONED FROM USER_TABLES WHERE TABLE_NAME = 'LOAD_TEST';
SELECT PARTITION_NAME FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = 'LOAD_TEST';
"""


# ============================================================================
# PostgreSQL 어댑터
# ============================================================================
class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL 데이터베이스 어댑터"""
    
    def __init__(self):
        if not POSTGRES_AVAILABLE:
            raise ImportError("psycopg2 module not available. Install with: pip install psycopg2-binary")
        self.pool = None
        
    def create_connection_pool(self, config: 'DatabaseConfig'):
        logger.info(f"Creating PostgreSQL connection pool (min={config.min_pool_size}, max={config.max_pool_size})")
        
        # PostgreSQL connection string
        self.pool = pg_pool.ThreadedConnectionPool(
            minconn=config.min_pool_size,
            maxconn=config.max_pool_size,
            host=config.host,
            port=config.port or 5432,
            database=config.database,
            user=config.user,
            password=config.password
        )
        
        logger.info("PostgreSQL connection pool created")
        return self.pool
    
    def get_connection(self):
        return self.pool.getconn()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                self.pool.putconn(connection)
            except Exception as e:
                logger.debug(f"Error releasing PostgreSQL connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.closeall()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """PostgreSQL INSERT with RETURNING"""
        sql = """
        INSERT INTO load_test (thread_id, value_col, random_data, created_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
        """
        
        cursor.execute(sql, (thread_id, f'TEST_{thread_id}', random_data))
        result = cursor.fetchone()
        return result[0]
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        sql = "SELECT id, thread_id, value_col FROM load_test WHERE id = %s"
        cursor.execute(sql, (record_id,))
        return cursor.fetchone()
    
    def commit(self, connection):
        connection.commit()
    
    def rollback(self, connection):
        try:
            connection.rollback()
        except:
            pass
    
    def get_ddl(self) -> str:
        return """
-- ============================================================================
-- PostgreSQL DDL (PostgreSQL 10+)
-- ============================================================================

-- 1. 시퀀스는 SERIAL로 자동 생성됨

-- 2. HASH PARTITION 테이블 생성 (PostgreSQL 11+)
CREATE TABLE load_test (
    id           BIGSERIAL       PRIMARY KEY,
    thread_id    VARCHAR(50)     NOT NULL,
    value_col    VARCHAR(200),
    random_data  VARCHAR(1000),
    status       VARCHAR(20)     DEFAULT 'ACTIVE',
    created_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (id);

-- 3. 파티션 생성 (16개)
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

-- 4. 인덱스 생성
CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at);
CREATE INDEX idx_load_test_created ON load_test(created_at);

-- 5. 테이블 확인
SELECT schemaname, tablename, partitiontype FROM pg_partitioned_table pt
JOIN pg_class c ON pt.partrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE tablename = 'load_test';
"""


# ============================================================================
# MySQL 어댑터
# ============================================================================
class MySQLAdapter(DatabaseAdapter):
    """MySQL 데이터베이스 어댑터"""
    
    def __init__(self):
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python module not available. Install with: pip install mysql-connector-python")
        self.pool = None
        self.pool_name = 'load_test_pool'
        
    def create_connection_pool(self, config: 'DatabaseConfig'):
        logger.info(f"Creating MySQL connection pool (size={config.max_pool_size})")
        
        # MySQL connection pool
        self.pool = mysql_pooling.MySQLConnectionPool(
            pool_name=self.pool_name,
            pool_size=min(config.max_pool_size, 32),  # MySQL has max 32 per pool
            pool_reset_session=True,
            host=config.host,
            port=config.port or 3306,
            database=config.database,
            user=config.user,
            password=config.password,
            autocommit=False
        )
        
        logger.info("MySQL connection pool created")
        return self.pool
    
    def get_connection(self):
        return self.pool.get_connection()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                connection.close()
            except Exception as e:
                logger.debug(f"Error releasing MySQL connection: {e}")
    
    def close_pool(self):
        # MySQL pool doesn't have explicit close
        pass
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """MySQL INSERT with AUTO_INCREMENT"""
        sql = """
        INSERT INTO load_test (thread_id, value_col, random_data, created_at)
        VALUES (%s, %s, %s, NOW())
        """
        
        cursor.execute(sql, (thread_id, f'TEST_{thread_id}', random_data))
        return cursor.lastrowid
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        sql = "SELECT id, thread_id, value_col FROM load_test WHERE id = %s"
        cursor.execute(sql, (record_id,))
        return cursor.fetchone()
    
    def commit(self, connection):
        connection.commit()
    
    def rollback(self, connection):
        try:
            connection.rollback()
        except:
            pass
    
    def get_ddl(self) -> str:
        return """
-- ============================================================================
-- MySQL DDL (MySQL 5.7+)
-- ============================================================================

-- 1. HASH PARTITION 테이블 생성 (16개 파티션)
CREATE TABLE load_test (
    id           BIGINT          NOT NULL AUTO_INCREMENT,
    thread_id    VARCHAR(50)     NOT NULL,
    value_col    VARCHAR(200),
    random_data  VARCHAR(1000),
    status       VARCHAR(20)     DEFAULT 'ACTIVE',
    created_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB
PARTITION BY HASH(id)
PARTITIONS 16;

-- 2. 인덱스 생성
CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at);
CREATE INDEX idx_load_test_created ON load_test(created_at);

-- 3. 테이블 확인
SHOW CREATE TABLE load_test;
"""


# ============================================================================
# SQL Server 어댑터
# ============================================================================
class SQLServerAdapter(DatabaseAdapter):
    """SQL Server 데이터베이스 어댑터"""
    
    def __init__(self):
        if not SQLSERVER_AVAILABLE:
            raise ImportError("pyodbc module not available. Install with: pip install pyodbc")
        self.connection_queue = None
        self.max_pool_size = 0
        self.connection_string = None
        
    def create_connection_pool(self, config: 'DatabaseConfig'):
        logger.info(f"Creating SQL Server connection pool (max={config.max_pool_size})")
        
        # SQL Server connection string
        if config.port:
            server = f"{config.host},{config.port}"
        else:
            server = config.host
        
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={config.database};"
            f"UID={config.user};"
            f"PWD={config.password};"
            f"TrustServerCertificate=yes;"
        )
        
        # 간단한 커넥션 풀 구현 (Queue 기반)
        self.max_pool_size = config.max_pool_size
        self.connection_queue = queue.Queue(maxsize=config.max_pool_size)
        
        # 초기 커넥션 생성
        for _ in range(config.min_pool_size):
            try:
                conn = pyodbc.connect(self.connection_string)
                conn.autocommit = False
                self.connection_queue.put(conn)
            except Exception as e:
                logger.warning(f"Failed to create initial connection: {e}")
        
        logger.info(f"SQL Server connection pool created with {self.connection_queue.qsize()} connections")
        return self.connection_queue
    
    def get_connection(self):
        try:
            # 큐에서 커넥션 가져오기 (타임아웃 5초)
            conn = self.connection_queue.get(timeout=5)
            return conn
        except queue.Empty:
            # 큐가 비어있으면 새 커넥션 생성
            logger.debug("Pool empty, creating new connection")
            conn = pyodbc.connect(self.connection_string)
            conn.autocommit = False
            return conn
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                
                # 큐가 가득 차지 않았으면 반환
                if self.connection_queue.qsize() < self.max_pool_size:
                    self.connection_queue.put_nowait(connection)
                else:
                    connection.close()
            except Exception as e:
                logger.debug(f"Error releasing SQL Server connection: {e}")
                try:
                    connection.close()
                except:
                    pass
    
    def close_pool(self):
        if self.connection_queue:
            while not self.connection_queue.empty():
                try:
                    conn = self.connection_queue.get_nowait()
                    conn.close()
                except:
                    pass
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """SQL Server INSERT with OUTPUT"""
        sql = """
        INSERT INTO load_test (thread_id, value_col, random_data, created_at)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?, GETDATE())
        """
        
        cursor.execute(sql, (thread_id, f'TEST_{thread_id}', random_data))
        result = cursor.fetchone()
        return result[0]
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        sql = "SELECT id, thread_id, value_col FROM load_test WHERE id = ?"
        cursor.execute(sql, (record_id,))
        return cursor.fetchone()
    
    def commit(self, connection):
        connection.commit()
    
    def rollback(self, connection):
        try:
            connection.rollback()
        except:
            pass
    
    def get_ddl(self) -> str:
        return """
-- ============================================================================
-- SQL Server DDL (SQL Server 2016+)
-- ============================================================================

-- 1. 파티션 함수 생성 (16개 파티션)
CREATE PARTITION FUNCTION PF_LoadTest (BIGINT)
AS RANGE LEFT FOR VALUES (
    625000000, 1250000000, 1875000000, 2500000000, 3125000000,
    3750000000, 4375000000, 5000000000, 5625000000, 6250000000,
    6875000000, 7500000000, 8125000000, 8750000000, 9375000000
);
GO

-- 2. 파티션 스키마 생성
CREATE PARTITION SCHEME PS_LoadTest
AS PARTITION PF_LoadTest
ALL TO ([PRIMARY]);
GO

-- 3. 테이블 생성
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

-- 4. 인덱스 생성
CREATE NONCLUSTERED INDEX idx_load_test_thread 
ON load_test(thread_id, created_at);
GO

CREATE NONCLUSTERED INDEX idx_load_test_created 
ON load_test(created_at);
GO

-- 5. 테이블 확인
SELECT 
    t.name AS TableName,
    i.name AS IndexName,
    p.partition_number,
    p.rows
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE t.name = 'load_test'
ORDER BY p.partition_number;
GO
"""


# ============================================================================
# Tibero 어댑터
# ============================================================================
class TiberoAdapter(DatabaseAdapter):
    """Tibero 데이터베이스 어댑터 (Oracle 호환)"""
    
    def __init__(self):
        if not TIBERO_AVAILABLE:
            raise ImportError("oracledb module not available. Install with: pip install oracledb")
        self.pool = None
        
    def create_connection_pool(self, config: 'DatabaseConfig'):
        logger.info(f"Creating Tibero session pool (min={config.min_pool_size}, max={config.max_pool_size})")
        
        # Tibero는 Oracle 드라이버와 호환
        self.pool = oracledb.create_pool(
            user=config.user,
            password=config.password,
            dsn=config.host,
            min=config.min_pool_size,
            max=config.max_pool_size,
            increment=config.pool_increment,
            getmode=oracledb.POOL_GETMODE_WAIT,
            threaded=True
        )
        
        logger.info(f"Tibero session pool created. Current size: {self.pool.opened}")
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing Tibero connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """Tibero INSERT with SEQUENCE"""
        sql = """
        INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
        VALUES (LOAD_TEST_SEQ.NEXTVAL, :thread_id, :value_col, :random_data, SYSTIMESTAMP)
        RETURNING ID INTO :new_id
        """
        
        new_id_var = cursor.var(int)
        cursor.execute(sql, {
            'thread_id': thread_id,
            'value_col': f'TEST_{thread_id}',
            'random_data': random_data,
            'new_id': new_id_var
        })
        
        return new_id_var.getvalue()[0]
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        sql = "SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = :id"
        cursor.execute(sql, {'id': record_id})
        return cursor.fetchone()
    
    def commit(self, connection):
        connection.commit()
    
    def rollback(self, connection):
        try:
            connection.rollback()
        except:
            pass
    
    def get_ddl(self) -> str:
        return """
-- ============================================================================
-- Tibero DDL (Oracle 호환 문법)
-- ============================================================================

-- 1. SEQUENCE 생성
CREATE SEQUENCE LOAD_TEST_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 1000
    NOCYCLE
    ORDER;

-- 2. HASH PARTITION 테이블 생성 (16개 파티션)
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
TABLESPACE USR_DATA
ENABLE ROW MOVEMENT;

-- 3. PRIMARY KEY 제약조건
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

-- 4. 인덱스 생성
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL;

-- 5. 통계 수집 (선택사항)
-- EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'LOAD_TEST', CASCADE => TRUE);

-- 6. 테이블 확인
SELECT TABLE_NAME, PARTITIONED FROM USER_TABLES WHERE TABLE_NAME = 'LOAD_TEST';
SELECT PARTITION_NAME FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = 'LOAD_TEST';
"""


# ============================================================================
# 설정 클래스
# ============================================================================
@dataclass
class DatabaseConfig:
    """데이터베이스 연결 설정"""
    db_type: str  # 'oracle', 'postgresql', 'mysql', 'sqlserver', 'tibero'
    host: str
    user: str
    password: str
    database: Optional[str] = None
    port: Optional[int] = None
    min_pool_size: int = 100
    max_pool_size: int = 200
    pool_increment: int = 10


# ============================================================================
# 부하 테스트 워커
# ============================================================================
class LoadTestWorker:
    """부하 테스트 워커 클래스"""
    
    def __init__(self, worker_id: int, db_adapter: DatabaseAdapter, end_time: datetime):
        self.worker_id = worker_id
        self.db_adapter = db_adapter
        self.end_time = end_time
        self.thread_name = f"Worker-{worker_id:04d}"
        self.transaction_count = 0
        
    def generate_random_data(self, length: int = 500) -> str:
        """랜덤 데이터 생성"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def execute_transaction(self, connection) -> bool:
        """단일 트랜잭션 실행 (INSERT -> COMMIT -> SELECT -> VERIFY)"""
        cursor = None
        try:
            cursor = connection.cursor()
            
            # 1. INSERT
            thread_id = self.thread_name
            random_data = self.generate_random_data()
            
            new_id = self.db_adapter.execute_insert(cursor, thread_id, random_data)
            perf_counter.increment_insert()
            
            # 2. COMMIT
            self.db_adapter.commit(connection)
            
            # 3. SELECT (검증)
            result = self.db_adapter.execute_select(cursor, new_id)
            perf_counter.increment_select()
            
            # 4. VERIFY
            if result is None or result[0] != new_id:
                logger.warning(f"[{self.thread_name}] Verification failed for ID={new_id}")
                perf_counter.increment_verification_failure()
                return False
            
            self.transaction_count += 1
            return True
            
        except Exception as e:
            logger.error(f"[{self.thread_name}] Transaction error: {str(e)}")
            perf_counter.increment_error()
            self.db_adapter.rollback(connection)
            return False
            
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    def run(self) -> int:
        """워커 실행 (종료 시간까지 반복)"""
        logger.info(f"[{self.thread_name}] Starting worker")
        
        connection = None
        consecutive_errors = 0
        
        while datetime.now() < self.end_time:
            try:
                # 커넥션이 없으면 새로 획득
                if connection is None:
                    connection = self.db_adapter.get_connection()
                    consecutive_errors = 0
                
                # 트랜잭션 실행
                success = self.execute_transaction(connection)
                
                if not success:
                    consecutive_errors += 1
                    if consecutive_errors >= 5:
                        # 연속 에러 발생 시 커넥션 교체
                        logger.warning(f"[{self.thread_name}] Too many errors, recreating connection")
                        self.db_adapter.release_connection(connection, is_error=True)
                        connection = None
                        perf_counter.increment_connection_recreate()
                        time.sleep(0.5)
                else:
                    consecutive_errors = 0
                
                # 부하 조절 (선택사항)
                # time.sleep(0.001)
                
            except Exception as e:
                logger.error(f"[{self.thread_name}] Connection error: {str(e)}")
                perf_counter.increment_error()
                
                # 커넥션 정리
                if connection:
                    self.db_adapter.release_connection(connection, is_error=True)
                    connection = None
                    perf_counter.increment_connection_recreate()
                
                time.sleep(0.5)
        
        # 정리
        if connection:
            self.db_adapter.release_connection(connection)
        
        logger.info(f"[{self.thread_name}] Worker completed. Total transactions: {self.transaction_count}")
        return self.transaction_count


# ============================================================================
# 모니터링 스레드
# ============================================================================
class MonitorThread(threading.Thread):
    """모니터링 스레드 - 주기적으로 통계 출력"""
    
    def __init__(self, interval_seconds: int, end_time: datetime):
        super().__init__(name="Monitor", daemon=True)
        self.interval_seconds = interval_seconds
        self.end_time = end_time
        self.running = True
        self.last_inserts = 0
        self.last_time = time.time()
        
    def run(self):
        """모니터링 실행"""
        logger.info("[Monitor] Starting performance monitor")
        
        while self.running and datetime.now() < self.end_time:
            time.sleep(self.interval_seconds)
            
            stats = perf_counter.get_stats()
            current_time = time.time()
            
            # 구간 TPS 계산
            interval_inserts = stats['total_inserts'] - self.last_inserts
            interval_time = current_time - self.last_time
            interval_tps = interval_inserts / interval_time if interval_time > 0 else 0
            
            logger.info(
                f"[Monitor] Stats - "
                f"Inserts: {stats['total_inserts']:,} | "
                f"Selects: {stats['total_selects']:,} | "
                f"Errors: {stats['total_errors']:,} | "
                f"Ver.Fail: {stats['verification_failures']:,} | "
                f"Conn.Recreate: {stats['connection_recreates']:,} | "
                f"Avg TPS: {stats['tps']:.2f} | "
                f"Interval TPS: {interval_tps:.2f} | "
                f"Elapsed: {stats['elapsed_seconds']:.1f}s"
            )
            
            self.last_inserts = stats['total_inserts']
            self.last_time = current_time
        
        logger.info("[Monitor] Stopping performance monitor")
    
    def stop(self):
        self.running = False


# ============================================================================
# 부하 테스터 메인 클래스
# ============================================================================
class MultiDBLoadTester:
    """멀티 데이터베이스 부하 테스터"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.db_adapter = self._create_adapter()
        
    def _create_adapter(self) -> DatabaseAdapter:
        """DB 타입에 따른 어댑터 생성"""
        db_type = self.config.db_type.lower()
        
        if db_type == 'oracle':
            return OracleAdapter()
        elif db_type in ['postgresql', 'postgres', 'pg']:
            return PostgreSQLAdapter()
        elif db_type == 'mysql':
            return MySQLAdapter()
        elif db_type in ['sqlserver', 'mssql', 'sql_server']:
            return SQLServerAdapter()
        elif db_type == 'tibero':
            return TiberoAdapter()
        else:
            raise ValueError(f"Unsupported database type: {self.config.db_type}")
    
    def print_ddl(self):
        """DDL 출력"""
        print("\n" + "="*80)
        print(f"DDL for {self.config.db_type.upper()}")
        print("="*80)
        print(self.db_adapter.get_ddl())
        print("="*80 + "\n")
    
    def run_load_test(self, thread_count: int, duration_seconds: int):
        """부하 테스트 실행"""
        logger.info(f"Starting load test: {thread_count} threads for {duration_seconds} seconds")
        
        # 커넥션 풀 생성
        self.db_adapter.create_connection_pool(self.config)
        
        # 종료 시간 설정
        end_time = datetime.now() + timedelta(seconds=duration_seconds)
        
        # 모니터링 스레드 시작
        monitor = MonitorThread(interval_seconds=5, end_time=end_time)
        monitor.start()
        
        # 워커 스레드 실행
        total_transactions = 0
        with ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="Worker") as executor:
            futures = []
            for i in range(thread_count):
                worker = LoadTestWorker(i + 1, self.db_adapter, end_time)
                future = executor.submit(worker.run)
                futures.append(future)
            
            # 모든 워커 완료 대기
            for future in as_completed(futures):
                try:
                    result = future.result()
                    total_transactions += result
                except Exception as e:
                    logger.error(f"Worker thread failed: {str(e)}")
        
        # 모니터링 스레드 정지
        monitor.stop()
        monitor.join(timeout=5)
        
        # 최종 통계 출력
        self._print_final_stats(thread_count, duration_seconds, total_transactions)
        
        # 풀 정리
        self.db_adapter.close_pool()
    
    def _print_final_stats(self, thread_count: int, duration_seconds: int, total_transactions: int):
        """최종 통계 출력"""
        final_stats = perf_counter.get_stats()
        
        logger.info("="*80)
        logger.info("LOAD TEST COMPLETED - FINAL STATISTICS")
        logger.info("="*80)
        logger.info(f"Database Type: {self.config.db_type.upper()}")
        logger.info(f"Total Threads: {thread_count}")
        logger.info(f"Test Duration: {duration_seconds} seconds")
        logger.info(f"Actual Elapsed: {final_stats['elapsed_seconds']:.1f} seconds")
        logger.info("-"*80)
        logger.info(f"Total Inserts: {final_stats['total_inserts']:,}")
        logger.info(f"Total Selects: {final_stats['total_selects']:,}")
        logger.info(f"Total Errors: {final_stats['total_errors']:,}")
        logger.info(f"Verification Failures: {final_stats['verification_failures']:,}")
        logger.info(f"Connection Recreates: {final_stats['connection_recreates']:,}")
        logger.info("-"*80)
        logger.info(f"Average TPS: {final_stats['tps']:.2f}")
        logger.info(f"Transactions per Thread: {total_transactions / thread_count:.2f}")
        logger.info(f"Success Rate: {((final_stats['total_inserts'] - final_stats['total_errors']) / final_stats['total_inserts'] * 100):.2f}%" if final_stats['total_inserts'] > 0 else "N/A")
        logger.info("="*80)


# ============================================================================
# 명령행 인자 파싱
# ============================================================================
def parse_arguments():
    """명령행 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='Multi-Database Load Tester (Oracle, PostgreSQL, MySQL, SQL Server, Tibero)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Oracle
  python multi_db_load_tester.py --db-type oracle \\
      --host "DSN" --user user --password pass --thread-count 200

  # PostgreSQL
  python multi_db_load_tester.py --db-type postgresql \\
      --host localhost --port 5432 --database testdb \\
      --user user --password pass --thread-count 200

  # MySQL
  python multi_db_load_tester.py --db-type mysql \\
      --host localhost --database testdb --user user --password pass

  # SQL Server
  python multi_db_load_tester.py --db-type sqlserver \\
      --host localhost --database testdb --user sa --password pass

  # Tibero
  python multi_db_load_tester.py --db-type tibero \\
      --host "DSN" --user user --password pass --thread-count 200
        """
    )
    
    # 데이터베이스 타입
    parser.add_argument(
        '--db-type',
        required=True,
        choices=['oracle', 'postgresql', 'postgres', 'pg', 'mysql', 'sqlserver', 'mssql', 'tibero'],
        help='Database type'
    )
    
    # 연결 정보
    parser.add_argument('--host', help='Database host (DSN for Oracle/Tibero)')
    parser.add_argument('--port', type=int, help='Database port')
    parser.add_argument('--database', help='Database name (PostgreSQL, MySQL, SQL Server)')
    parser.add_argument('--user', help='Database username')
    parser.add_argument('--password', help='Database password')
    
    # 풀 설정
    parser.add_argument('--min-pool-size', type=int, default=100, help='Minimum pool size')
    parser.add_argument('--max-pool-size', type=int, default=200, help='Maximum pool size')
    parser.add_argument('--pool-increment', type=int, default=10, help='Pool increment')
    
    # 테스트 설정
    parser.add_argument('--thread-count', type=int, default=100, help='Number of worker threads')
    parser.add_argument('--test-duration', type=int, default=300, help='Test duration (seconds)')
    
    # 기타
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--print-ddl', action='store_true', help='Print DDL and exit')
    
    return parser.parse_args()


# ============================================================================
# 메인 함수
# ============================================================================
def main():
    """메인 함수"""
    args = parse_arguments()
    
    # 로깅 레벨 설정
    logger.setLevel(getattr(logging, args.log_level))
    
    # 데이터베이스 설정 생성
    config = DatabaseConfig(
        db_type=args.db_type,
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
        min_pool_size=args.min_pool_size,
        max_pool_size=args.max_pool_size,
        pool_increment=args.pool_increment
    )
    
    # 테스터 생성
    tester = MultiDBLoadTester(config)
    
    # DDL 출력 모드
    if args.print_ddl:
        tester.print_ddl()
        return
    
    # 필수 인자 확인
    if not all([args.host, args.user, args.password]):
        logger.error("--host, --user, --password are required for load test")
        sys.exit(1)
    
    db_type = args.db_type.lower()
    if db_type in ['postgresql', 'postgres', 'pg', 'mysql', 'sqlserver', 'mssql'] and not args.database:
        logger.error(f"--database is required for {args.db_type}")
        sys.exit(1)
    
    # 설정 출력
    logger.info("="*80)
    logger.info("MULTI-DATABASE LOAD TESTER CONFIGURATION")
    logger.info("="*80)
    logger.info(f"Database Type: {config.db_type.upper()}")
    logger.info(f"Host: {config.host}")
    if config.port:
        logger.info(f"Port: {config.port}")
    if config.database:
        logger.info(f"Database: {config.database}")
    logger.info(f"User: {config.user}")
    logger.info(f"Min Pool Size: {config.min_pool_size}")
    logger.info(f"Max Pool Size: {config.max_pool_size}")
    logger.info(f"Thread Count: {args.thread_count}")
    logger.info(f"Test Duration: {args.test_duration} seconds")
    logger.info("="*80)
    
    # 부하 테스트 실행
    try:
        tester.run_load_test(
            thread_count=args.thread_count,
            duration_seconds=args.test_duration
        )
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
