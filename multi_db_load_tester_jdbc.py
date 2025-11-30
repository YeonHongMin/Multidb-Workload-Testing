#!/usr/bin/env python3
"""
멀티 데이터베이스 부하 테스트 프로그램 (JDBC 드라이버 사용)
Oracle, PostgreSQL, MySQL, SQL Server, Tibero 지원

특징:
- ./jre 디렉터리의 JDBC 드라이버 사용
- JayDeBeApi를 통한 JDBC 연결
- 멀티스레드 + 커넥션 풀링
- INSERT -> COMMIT -> SELECT 검증 패턴
- 자동 에러 복구 및 커넥션 재연결
- 실시간 성능 모니터링 (TPS, 에러 카운트)
"""

import sys
import time
import logging
import threading
import argparse
import random
import string
import os
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from abc import ABC, abstractmethod
import queue

# JDBC 드라이버 사용을 위한 라이브러리
try:
    import jaydebeapi
    JAYDEBEAPI_AVAILABLE = True
except ImportError:
    JAYDEBEAPI_AVAILABLE = False
    print("ERROR: jaydebeapi not installed. Install with: pip install jaydebeapi")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)-15s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_db_load_test_jdbc.log'),
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
# JDBC 드라이버 정보
# ============================================================================
@dataclass
class JDBCDriverInfo:
    """JDBC 드라이버 정보"""
    driver_class: str
    jar_pattern: str
    url_template: str


JDBC_DRIVERS = {
    'oracle': JDBCDriverInfo(
        driver_class='oracle.jdbc.OracleDriver',
        jar_pattern='ojdbc*.jar',
        url_template='jdbc:oracle:thin:@{host}:{port}:{sid}'
    ),
    'tibero': JDBCDriverInfo(
        driver_class='com.tmax.tibero.jdbc.TbDriver',
        jar_pattern='tibero*jdbc*.jar',
        url_template='jdbc:tibero:thin:@{host}:{port}:{sid}'
    ),
    'postgresql': JDBCDriverInfo(
        driver_class='org.postgresql.Driver',
        jar_pattern='postgresql-*.jar',
        url_template='jdbc:postgresql://{host}:{port}/{database}'
    ),
    'mysql': JDBCDriverInfo(
        driver_class='com.mysql.cj.jdbc.Driver',
        jar_pattern='mysql-connector-*.jar',
        url_template='jdbc:mysql://{host}:{port}/{database}'
    ),
    'sqlserver': JDBCDriverInfo(
        driver_class='com.microsoft.sqlserver.jdbc.SQLServerDriver',
        jar_pattern='mssql-jdbc-*.jar',
        url_template='jdbc:sqlserver://{host}:{port};databaseName={database}'
    )
}


def find_jdbc_jar(db_type: str, jre_dir: str = './jre') -> Optional[str]:
    """./jre 디렉터리에서 JDBC JAR 파일 찾기"""
    if db_type not in JDBC_DRIVERS:
        raise ValueError(f"Unsupported DB type: {db_type}")
    
    driver_info = JDBC_DRIVERS[db_type]
    pattern = os.path.join(jre_dir, '**', driver_info.jar_pattern)
    
    jar_files = glob.glob(pattern, recursive=True)
    
    if not jar_files:
        logger.error(f"JDBC driver not found: {driver_info.jar_pattern} in {jre_dir}")
        return None
    
    # 여러 개 있으면 가장 최신 버전 사용
    jar_file = sorted(jar_files)[-1]
    logger.info(f"Found JDBC driver: {jar_file}")
    return jar_file


# ============================================================================
# JDBC 커넥션 풀 (Queue 기반)
# ============================================================================
class JDBCConnectionPool:
    """JDBC 커넥션 풀"""
    
    def __init__(self, jdbc_url: str, driver_class: str, jar_file: str, 
                 user: str, password: str, min_size: int, max_size: int):
        self.jdbc_url = jdbc_url
        self.driver_class = driver_class
        self.jar_file = jar_file
        self.user = user
        self.password = password
        self.min_size = min_size
        self.max_size = max_size
        
        self.pool = queue.Queue(maxsize=max_size)
        self.current_size = 0
        self.lock = threading.Lock()
        
        logger.info(f"Initializing JDBC connection pool (min={min_size}, max={max_size})")
        logger.info(f"JDBC URL: {jdbc_url}")
        logger.info(f"Driver Class: {driver_class}")
        logger.info(f"JAR File: {jar_file}")
        
        # 초기 커넥션 생성
        for _ in range(min_size):
            self._create_connection()
    
    def _create_connection(self):
        """새 JDBC 커넥션 생성"""
        with self.lock:
            if self.current_size >= self.max_size:
                return
            
            try:
                conn = jaydebeapi.connect(
                    self.driver_class,
                    self.jdbc_url,
                    [self.user, self.password],
                    self.jar_file
                )
                conn.jconn.setAutoCommit(False)  # 명시적 커밋
                self.pool.put(conn)
                self.current_size += 1
                logger.debug(f"Created new connection. Pool size: {self.current_size}")
            except Exception as e:
                logger.error(f"Failed to create connection: {e}")
    
    def acquire(self, timeout: int = 30):
        """커넥션 획득"""
        try:
            conn = self.pool.get(timeout=timeout)
            return conn
        except queue.Empty:
            # 풀이 비어있고 최대 크기 미만이면 새로 생성
            with self.lock:
                if self.current_size < self.max_size:
                    conn = jaydebeapi.connect(
                        self.driver_class,
                        self.jdbc_url,
                        [self.user, self.password],
                        self.jar_file
                    )
                    conn.jconn.setAutoCommit(False)
                    self.current_size += 1
                    logger.debug(f"Created connection on demand. Pool size: {self.current_size}")
                    return conn
            
            # 생성 불가능하면 다시 대기
            return self.pool.get(timeout=timeout)
    
    def release(self, conn):
        """커넥션 반환"""
        if conn is None:
            return
        
        try:
            # 풀이 가득 차지 않았으면 반환
            if self.pool.qsize() < self.max_size:
                self.pool.put_nowait(conn)
            else:
                # 풀이 가득 찼으면 연결 종료
                try:
                    conn.close()
                except:
                    pass
                with self.lock:
                    self.current_size -= 1
        except queue.Full:
            try:
                conn.close()
            except:
                pass
            with self.lock:
                self.current_size -= 1
    
    def close_all(self):
        """모든 커넥션 종료"""
        logger.info("Closing all connections in pool...")
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass
        logger.info("All connections closed")


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
# Oracle JDBC 어댑터
# ============================================================================
class OracleJDBCAdapter(DatabaseAdapter):
    """Oracle JDBC 어댑터"""
    
    def __init__(self, jre_dir: str = './jre'):
        self.pool = None
        self.jar_file = find_jdbc_jar('oracle', jre_dir)
        if not self.jar_file:
            raise RuntimeError("Oracle JDBC driver not found in ./jre directory")
    
    def create_connection_pool(self, config: 'DatabaseConfig'):
        # JDBC URL 생성
        jdbc_url = JDBC_DRIVERS['oracle'].url_template.format(
            host=config.host,
            port=config.port or 1521,
            sid=config.sid or config.database
        )
        
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url,
            driver_class=JDBC_DRIVERS['oracle'].driver_class,
            jar_file=self.jar_file,
            user=config.user,
            password=config.password,
            min_size=config.min_pool_size,
            max_size=config.max_pool_size
        )
        
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close_all()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """Oracle INSERT with SEQUENCE"""
        # RETURNING 구문을 사용하지 않고 CURRVAL로 조회
        cursor.execute("""
            INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
            VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, SYSTIMESTAMP)
        """, [thread_id, f'TEST_{thread_id}', random_data])
        
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        result = cursor.fetchone()
        return int(result[0])
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [record_id])
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
-- Oracle DDL (JDBC)
-- ============================================================================

CREATE SEQUENCE LOAD_TEST_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 1000
    NOCYCLE
    ORDER;

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

ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL;
"""


# ============================================================================
# PostgreSQL JDBC 어댑터
# ============================================================================
class PostgreSQLJDBCAdapter(DatabaseAdapter):
    """PostgreSQL JDBC 어댑터"""
    
    def __init__(self, jre_dir: str = './jre'):
        self.pool = None
        self.jar_file = find_jdbc_jar('postgresql', jre_dir)
        if not self.jar_file:
            raise RuntimeError("PostgreSQL JDBC driver not found in ./jre directory")
    
    def create_connection_pool(self, config: 'DatabaseConfig'):
        jdbc_url = JDBC_DRIVERS['postgresql'].url_template.format(
            host=config.host,
            port=config.port or 5432,
            database=config.database
        )
        
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url,
            driver_class=JDBC_DRIVERS['postgresql'].driver_class,
            jar_file=self.jar_file,
            user=config.user,
            password=config.password,
            min_size=config.min_pool_size,
            max_size=config.max_pool_size
        )
        
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close_all()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """PostgreSQL INSERT with RETURNING"""
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, [thread_id, f'TEST_{thread_id}', random_data])
        
        result = cursor.fetchone()
        return int(result[0])
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
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
-- PostgreSQL DDL (JDBC)
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

-- 16 partitions
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

CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at);
CREATE INDEX idx_load_test_created ON load_test(created_at);
"""


# ============================================================================
# MySQL JDBC 어댑터
# ============================================================================
class MySQLJDBCAdapter(DatabaseAdapter):
    """MySQL JDBC 어댑터"""
    
    def __init__(self, jre_dir: str = './jre'):
        self.pool = None
        self.jar_file = find_jdbc_jar('mysql', jre_dir)
        if not self.jar_file:
            raise RuntimeError("MySQL JDBC driver not found in ./jre directory")
    
    def create_connection_pool(self, config: 'DatabaseConfig'):
        jdbc_url = JDBC_DRIVERS['mysql'].url_template.format(
            host=config.host,
            port=config.port or 3306,
            database=config.database
        )
        
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url,
            driver_class=JDBC_DRIVERS['mysql'].driver_class,
            jar_file=self.jar_file,
            user=config.user,
            password=config.password,
            min_size=min(config.min_pool_size, 32),
            max_size=min(config.max_pool_size, 32)  # MySQL 제한
        )
        
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close_all()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """MySQL INSERT with AUTO_INCREMENT"""
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, NOW())
        """, [thread_id, f'TEST_{thread_id}', random_data])
        
        cursor.execute("SELECT LAST_INSERT_ID()")
        result = cursor.fetchone()
        return int(result[0])
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
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
-- MySQL DDL (JDBC)
-- ============================================================================

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

CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at);
CREATE INDEX idx_load_test_created ON load_test(created_at);
"""


# ============================================================================
# SQL Server JDBC 어댑터
# ============================================================================
class SQLServerJDBCAdapter(DatabaseAdapter):
    """SQL Server JDBC 어댑터"""
    
    def __init__(self, jre_dir: str = './jre'):
        self.pool = None
        self.jar_file = find_jdbc_jar('sqlserver', jre_dir)
        if not self.jar_file:
            raise RuntimeError("SQL Server JDBC driver not found in ./jre directory")
    
    def create_connection_pool(self, config: 'DatabaseConfig'):
        jdbc_url = JDBC_DRIVERS['sqlserver'].url_template.format(
            host=config.host,
            port=config.port or 1433,
            database=config.database
        )
        
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url,
            driver_class=JDBC_DRIVERS['sqlserver'].driver_class,
            jar_file=self.jar_file,
            user=config.user,
            password=config.password,
            min_size=config.min_pool_size,
            max_size=config.max_pool_size
        )
        
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close_all()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """SQL Server INSERT with IDENTITY"""
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, GETDATE())
        """, [thread_id, f'TEST_{thread_id}', random_data])
        
        cursor.execute("SELECT SCOPE_IDENTITY()")
        result = cursor.fetchone()
        return int(result[0])
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
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
-- SQL Server DDL (JDBC)
-- ============================================================================

CREATE PARTITION FUNCTION PF_LoadTest (BIGINT)
AS RANGE LEFT FOR VALUES (
    625000000, 1250000000, 1875000000, 2500000000, 3125000000,
    3750000000, 4375000000, 5000000000, 5625000000, 6250000000,
    6875000000, 7500000000, 8125000000, 8750000000, 9375000000
);

CREATE PARTITION SCHEME PS_LoadTest
AS PARTITION PF_LoadTest
ALL TO ([PRIMARY]);

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

CREATE NONCLUSTERED INDEX idx_load_test_thread ON load_test(thread_id, created_at);
CREATE NONCLUSTERED INDEX idx_load_test_created ON load_test(created_at);
"""


# ============================================================================
# Tibero JDBC 어댑터
# ============================================================================
class TiberoJDBCAdapter(DatabaseAdapter):
    """Tibero JDBC 어댑터 (Oracle 호환)"""
    
    def __init__(self, jre_dir: str = './jre'):
        self.pool = None
        self.jar_file = find_jdbc_jar('tibero', jre_dir)
        if not self.jar_file:
            raise RuntimeError("Tibero JDBC driver not found in ./jre directory")
    
    def create_connection_pool(self, config: 'DatabaseConfig'):
        jdbc_url = JDBC_DRIVERS['tibero'].url_template.format(
            host=config.host,
            port=config.port or 8629,
            sid=config.sid or config.database
        )
        
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url,
            driver_class=JDBC_DRIVERS['tibero'].driver_class,
            jar_file=self.jar_file,
            user=config.user,
            password=config.password,
            min_size=config.min_pool_size,
            max_size=config.max_pool_size
        )
        
        return self.pool
    
    def get_connection(self):
        return self.pool.acquire()
    
    def release_connection(self, connection, is_error: bool = False):
        if connection:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except Exception as e:
                logger.debug(f"Error releasing connection: {e}")
    
    def close_pool(self):
        if self.pool:
            self.pool.close_all()
    
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """Tibero INSERT with SEQUENCE"""
        cursor.execute("""
            INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
            VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, SYSTIMESTAMP)
        """, [thread_id, f'TEST_{thread_id}', random_data])
        
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        result = cursor.fetchone()
        return int(result[0])
    
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [record_id])
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
-- Tibero DDL (JDBC)
-- ============================================================================

CREATE SEQUENCE LOAD_TEST_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 1000
    NOCYCLE
    ORDER;

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

ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL;
"""


# ============================================================================
# 설정 클래스
# ============================================================================
@dataclass
class DatabaseConfig:
    """데이터베이스 연결 설정"""
    db_type: str
    host: str
    user: str
    password: str
    database: Optional[str] = None
    sid: Optional[str] = None
    port: Optional[int] = None
    min_pool_size: int = 100
    max_pool_size: int = 200
    jre_dir: str = './jre'


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
            return OracleJDBCAdapter(self.config.jre_dir)
        elif db_type in ['postgresql', 'postgres', 'pg']:
            return PostgreSQLJDBCAdapter(self.config.jre_dir)
        elif db_type == 'mysql':
            return MySQLJDBCAdapter(self.config.jre_dir)
        elif db_type in ['sqlserver', 'mssql', 'sql_server']:
            return SQLServerJDBCAdapter(self.config.jre_dir)
        elif db_type == 'tibero':
            return TiberoJDBCAdapter(self.config.jre_dir)
        else:
            raise ValueError(f"Unsupported database type: {self.config.db_type}")
    
    def print_ddl(self):
        """DDL 출력"""
        print("\n" + "="*80)
        print(f"DDL for {self.config.db_type.upper()} (JDBC)")
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
        logger.info(f"Database Type: {self.config.db_type.upper()} (JDBC)")
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
        description='Multi-Database Load Tester using JDBC drivers from ./jre directory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Oracle
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test_user --password pass --thread-count 200

  # PostgreSQL
  python multi_db_load_tester_jdbc.py --db-type postgresql \\
      --host localhost --port 5432 --database testdb \\
      --user test_user --password pass --thread-count 200

  # MySQL
  python multi_db_load_tester_jdbc.py --db-type mysql \\
      --host localhost --database testdb \\
      --user test_user --password pass --thread-count 100

  # SQL Server
  python multi_db_load_tester_jdbc.py --db-type sqlserver \\
      --host localhost --database testdb \\
      --user sa --password pass --thread-count 200

  # Tibero
  python multi_db_load_tester_jdbc.py --db-type tibero \\
      --host localhost --port 8629 --sid tibero \\
      --user test_user --password pass --thread-count 200

JDBC Drivers Required in ./jre directory:
  - Oracle: ojdbc*.jar
  - Tibero: tibero*jdbc*.jar
  - PostgreSQL: postgresql-*.jar
  - MySQL: mysql-connector-*.jar
  - SQL Server: mssql-jdbc-*.jar
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
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', type=int, help='Database port')
    parser.add_argument('--database', help='Database name (PostgreSQL, MySQL, SQL Server)')
    parser.add_argument('--sid', help='SID or Service Name (Oracle, Tibero)')
    parser.add_argument('--user', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    
    # JRE 디렉터리
    parser.add_argument('--jre-dir', default='./jre', help='JRE/JDBC drivers directory')
    
    # 풀 설정
    parser.add_argument('--min-pool-size', type=int, default=100, help='Minimum pool size')
    parser.add_argument('--max-pool-size', type=int, default=200, help='Maximum pool size')
    
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
    
    # jaydebeapi 확인
    if not JAYDEBEAPI_AVAILABLE:
        logger.error("jaydebeapi is not installed. Install with: pip install jaydebeapi")
        logger.error("Also ensure JPype1 is installed: pip install JPype1")
        sys.exit(1)
    
    # 로깅 레벨 설정
    logger.setLevel(getattr(logging, args.log_level))
    
    # 데이터베이스 설정 생성
    config = DatabaseConfig(
        db_type=args.db_type,
        host=args.host,
        port=args.port,
        database=args.database,
        sid=args.sid,
        user=args.user,
        password=args.password,
        min_pool_size=args.min_pool_size,
        max_pool_size=args.max_pool_size,
        jre_dir=args.jre_dir
    )
    
    # JRE 디렉터리 확인
    if not os.path.exists(args.jre_dir):
        logger.error(f"JRE directory not found: {args.jre_dir}")
        sys.exit(1)
    
    # 테스터 생성
    try:
        tester = MultiDBLoadTester(config)
    except Exception as e:
        logger.error(f"Failed to create tester: {str(e)}")
        sys.exit(1)
    
    # DDL 출력 모드
    if args.print_ddl:
        tester.print_ddl()
        return
    
    # 설정 출력
    logger.info("="*80)
    logger.info("MULTI-DATABASE LOAD TESTER CONFIGURATION (JDBC)")
    logger.info("="*80)
    logger.info(f"Database Type: {config.db_type.upper()}")
    logger.info(f"Host: {config.host}")
    if config.port:
        logger.info(f"Port: {config.port}")
    if config.database:
        logger.info(f"Database: {config.database}")
    if config.sid:
        logger.info(f"SID: {config.sid}")
    logger.info(f"User: {config.user}")
    logger.info(f"JRE Directory: {config.jre_dir}")
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
