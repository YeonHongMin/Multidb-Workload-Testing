"""
[1] 데이터베이스별 DDL (스키마 생성) - 각 DB에 맞게 선택하여 실행하세요.

MySQL DDL:
-- 1. 테이블 생성 (AUTO_INCREMENT 사용, HASH PARTITION 16개)
CREATE TABLE LOAD_TEST (
    ID           BIGINT          NOT NULL AUTO_INCREMENT,
    THREAD_ID    VARCHAR(50)     NOT NULL,
    VALUE_COL1   VARCHAR(200),
    VALUE_COL2   VARCHAR(500),
    VALUE_COL3   DECIMAL(10,2),
    RANDOM_DATA  VARCHAR(1000),
    STATUS       VARCHAR(20)     DEFAULT 'ACTIVE',
    CREATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY HASH (ID)
PARTITIONS 16;

-- 추가 인덱스
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_VAL1 ON LOAD_TEST(THREAD_ID, VALUE_COL1);

PostgreSQL DDL:
-- 1. SEQUENCE 생성
CREATE SEQUENCE LOAD_TEST_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 1000;

-- 2. HASH PARTITION 테이블 생성 (16개 파티션)
CREATE TABLE LOAD_TEST (
    ID           BIGINT          NOT NULL DEFAULT nextval('LOAD_TEST_SEQ'),
    THREAD_ID    VARCHAR(50)     NOT NULL,
    VALUE_COL1   VARCHAR(200),
    VALUE_COL2   VARCHAR(500),
    VALUE_COL3   NUMERIC(10,2),
    RANDOM_DATA  VARCHAR(1000),
    STATUS       VARCHAR(20)     DEFAULT 'ACTIVE',
    CREATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (ID);

-- 파티션 생성 (PostgreSQL 10+)
DO $$
DECLARE
    i INTEGER := 1;
BEGIN
    FOR i IN 1..16 LOOP
        EXECUTE format('CREATE TABLE load_test_p%02s PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, (i-1));
    END LOOP;
END $$;

-- PRIMARY KEY (전체 테이블에)
ALTER TABLE LOAD_TEST ADD PRIMARY KEY (ID);

-- 추가 인덱스
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_VAL1 ON LOAD_TEST(THREAD_ID, VALUE_COL1);

-- SEQUENCE 소유권 설정
ALTER SEQUENCE LOAD_TEST_SEQ OWNED BY LOAD_TEST.ID;

Oracle DDL (기존 그대로):
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
    VALUE_COL1   VARCHAR2(200),
    VALUE_COL2   VARCHAR2(500),
    VALUE_COL3   NUMBER(10,2),
    RANDOM_DATA  VARCHAR2(1000),
    STATUS       VARCHAR(20)     DEFAULT 'ACTIVE',
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

-- 3. PRIMARY KEY 제약조건 추가
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

-- 4. 추가 인덱스 생성
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL;
CREATE INDEX IDX_LOAD_TEST_VAL1 ON LOAD_TEST(THREAD_ID, VALUE_COL1) LOCAL;

SQL Server DDL:
-- 1. HASH PARTITION 함수 및 스키마 생성 (SQL Server 2005+)
CREATE PARTITION FUNCTION PF_LOAD_TEST (BIGINT) 
AS RANGE RIGHT FOR VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

CREATE PARTITION SCHEME PS_LOAD_TEST 
AS PARTITION PF_LOAD_TEST 
TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]);

-- 2. 테이블 생성 (IDENTITY 사용)
CREATE TABLE LOAD_TEST (
    ID           BIGINT          IDENTITY(1,1) NOT NULL,
    THREAD_ID    NVARCHAR(50)    NOT NULL,
    VALUE_COL1   NVARCHAR(200),
    VALUE_COL2   NVARCHAR(500),
    VALUE_COL3   DECIMAL(10,2),
    RANDOM_DATA  NVARCHAR(1000),
    STATUS       NVARCHAR(20)    DEFAULT 'ACTIVE',
    CREATED_AT   DATETIME2       DEFAULT GETDATE(),
    UPDATED_AT   DATETIME2       DEFAULT GETDATE()
) ON PS_LOAD_TEST(ID);

-- PRIMARY KEY
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

-- 추가 인덱스
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_VAL1 ON LOAD_TEST(THREAD_ID, VALUE_COL1);

Tibero DDL (Oracle 호환, 기존 Oracle DDL 사용 가능):
-- Oracle DDL과 동일하게 사용 가능 (Tibero는 Oracle 호환성 높음)

[2] Python 소스코드 (multi_db_loader_grok.py) - JDBC 버전
"""
import sys
import os
import time
import random
import string
import logging
import argparse
import threading
import queue
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# JDBC 관련 라이브러리
try:
    import jaydebeapi
    import jpype
except ImportError:
    print("Error: jaydebeapi and jpype1 modules are required.")
    print("pip install jaydebeapi jpype1")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)-12s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_db_load_test_jdbc.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 성능 카운터
class PerformanceCounter:
    """스레드 안전 성능 카운터"""
    def __init__(self):
        self.lock = threading.Lock()
        self.total_inserts = 0
        self.total_selects = 0
        self.total_errors = 0
        self.verification_failures = 0
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
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            elapsed_time = time.time() - self.start_time
            tps = self.total_inserts / elapsed_time if elapsed_time > 0 else 0
            return {
                'total_inserts': self.total_inserts,
                'total_selects': self.total_selects,
                'total_errors': self.total_errors,
                'verification_failures': self.verification_failures,
                'elapsed_seconds': elapsed_time,
                'tps': round(tps, 2)
            }

perf_counter = PerformanceCounter()

@dataclass
class JdbcConfig:
    """JDBC 연결 설정"""
    db_type: str
    url: str
    user: str
    password: str
    driver_class: str
    jar_path: str
    min_pool_size: int = 10
    max_pool_size: int = 50

class JdbcConnectionPool:
    """간단한 JDBC 연결 풀"""
    def __init__(self, config: JdbcConfig):
        self.config = config
        self.pool = queue.Queue(maxsize=config.max_pool_size)
        self.created_connections = 0
        self.lock = threading.Lock()
        
        # 초기 풀 채우기
        logger.info(f"Initializing connection pool (Min: {config.min_pool_size}, Max: {config.max_pool_size})")
        for _ in range(config.min_pool_size):
            conn = self._create_connection()
            if conn:
                self.pool.put(conn)
                
    def _create_connection(self):
        try:
            conn = jaydebeapi.connect(
                self.config.driver_class,
                self.config.url,
                [self.config.user, self.config.password],
                self.config.jar_path
            )
            # Disable autocommit
            try:
                if hasattr(conn, 'jconn'):
                    conn.jconn.setAutoCommit(False)
            except Exception as e:
                logger.warning(f"Failed to disable autocommit: {e}")

            with self.lock:
                self.created_connections += 1
            return conn
        except Exception as e:
            logger.error(f"Failed to create JDBC connection: {e}")
            return None

    def get_connection(self):
        try:
            conn = self.pool.get(block=False)
        except queue.Empty:
            with self.lock:
                if self.created_connections < self.config.max_pool_size:
                    return self._create_connection()
            
            # 풀이 꽉 찼으면 대기
            try:
                conn = self.pool.get(timeout=5)
            except queue.Empty:
                raise Exception("Connection pool exhausted")

        # Validate connection
        try:
            if conn.jconn.isClosed() or not conn.jconn.isValid(1):
                logger.warning("Discarding invalid connection")
                try:
                    conn.close()
                except:
                    pass
                with self.lock:
                    self.created_connections -= 1
                return self.get_connection() # Retry
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            try:
                conn.close()
            except:
                pass
            with self.lock:
                self.created_connections -= 1
            return self.get_connection() # Retry
            
        return conn

    def return_connection(self, conn):
        try:
            if conn:
                if not conn.jconn.isClosed():
                    self.pool.put(conn)
                else:
                    with self.lock:
                        self.created_connections -= 1
        except Exception as e:
            logger.error(f"Error returning connection: {e}")
            try:
                conn.close()
            except:
                pass
            with self.lock:
                self.created_connections -= 1

    def close_all(self):
        logger.info("Closing all connections in pool")
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass

def init_jvm(jar_paths: List[str]):
    """JVM 초기화"""
    if jpype.isJVMStarted():
        return

    # Java Home 설정 (C:\jdk25)
    java_home = r"C:\jdk25"
    jvm_path = os.path.join(java_home, "bin", "server", "jvm.dll")
    
    if not os.path.exists(jvm_path):
        # Fallback to default lookup if specific path fails
        jvm_path = jpype.getDefaultJVMPath()
        logger.warning(f"Specified JVM not found at {jvm_path}, using default: {jvm_path}")

    logger.info(f"Starting JVM: {jvm_path}")
    
    # Classpath 설정
    classpath = [os.path.abspath(p) for p in jar_paths]
    logger.info(f"JVM Classpath: {classpath}")

    try:
        if not jpype.isJVMStarted():
            jpype.startJVM(jvm_path, f"-Djava.class.path={os.pathsep.join(classpath)}", "-Xmx4g")
    except Exception as e:
        logger.error(f"Failed to start JVM: {e}")
        sys.exit(1)

def setup_schema(config: JdbcConfig):
    """스키마 생성 (Drop & Create)"""
    logger.info(f"Setting up schema for {config.db_type}...")
    conn = None
    try:
        conn = jaydebeapi.connect(
            config.driver_class,
            config.url,
            [config.user, config.password],
            config.jar_path
        )
        cursor = conn.cursor()
        
        # 1. Drop Objects
        try:
            if config.db_type in ['oracle', 'tibero']:
                cursor.execute("DROP TABLE LOAD_TEST PURGE")
            else:
                cursor.execute("DROP TABLE LOAD_TEST")
        except Exception as e:
            logger.warning(f"Drop table failed (might not exist): {e}")

        try:
            cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
        except Exception as e:
            logger.warning(f"Drop sequence failed (might not exist): {e}")

        # 2. Create Objects
        if config.db_type in ['oracle', 'tibero']:
            # Sequence
            cursor.execute("""
                CREATE SEQUENCE LOAD_TEST_SEQ
                START WITH 1
                INCREMENT BY 1
                CACHE 1000
                NOCYCLE
                ORDER
            """)
            
            # Table
            cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID           NUMBER(19)      NOT NULL,
                    THREAD_ID    VARCHAR2(50)    NOT NULL,
                    VALUE_COL1   VARCHAR2(200),
                    VALUE_COL2   VARCHAR2(500),
                    VALUE_COL3   NUMBER(10,2),
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
            """)
            
            # PK
            cursor.execute("ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID)")
            
            # Indexes
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL")
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT) LOCAL")
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_VAL1 ON LOAD_TEST(THREAD_ID, VALUE_COL1) LOCAL")
            
        elif config.db_type == 'postgresql':
             # Sequence
            cursor.execute("""
                CREATE SEQUENCE LOAD_TEST_SEQ
                START WITH 1
                INCREMENT BY 1
                CACHE 1000
            """)
            
            # Table
            cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID           BIGINT          NOT NULL DEFAULT nextval('LOAD_TEST_SEQ'),
                    THREAD_ID    VARCHAR(50)     NOT NULL,
                    VALUE_COL1   VARCHAR(200),
                    VALUE_COL2   VARCHAR(500),
                    VALUE_COL3   NUMERIC(10,2),
                    RANDOM_DATA  VARCHAR(1000),
                    STATUS       VARCHAR(20)     DEFAULT 'ACTIVE',
                    CREATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
                    UPDATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
                ) PARTITION BY HASH (ID)
            """)
            
            # Partitions
            for i in range(1, 17):
                 cursor.execute(f"CREATE TABLE load_test_p{i:02d} PARTITION OF load_test FOR VALUES WITH (MODULUS 16, REMAINDER {i-1})")

            cursor.execute("ALTER TABLE LOAD_TEST ADD PRIMARY KEY (ID)")
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT)")
            
        elif config.db_type == 'mysql':
            cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID           BIGINT          NOT NULL AUTO_INCREMENT,
                    THREAD_ID    VARCHAR(50)     NOT NULL,
                    VALUE_COL1   VARCHAR(200),
                    VALUE_COL2   VARCHAR(500),
                    VALUE_COL3   DECIMAL(10,2),
                    RANDOM_DATA  VARCHAR(1000),
                    STATUS       VARCHAR(20)     DEFAULT 'ACTIVE',
                    CREATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
                    UPDATED_AT   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (ID)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                PARTITION BY HASH (ID)
                PARTITIONS 16
            """)
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT)")

        elif config.db_type == 'sqlserver':
             # Note: Partitioning in SQL Server requires more complex setup (Filegroups), skipping for simple test or using simplified table
             # For this script, we will create a simple table if partition scheme not pre-exists
             cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID           BIGINT          IDENTITY(1,1) NOT NULL,
                    THREAD_ID    NVARCHAR(50)    NOT NULL,
                    VALUE_COL1   NVARCHAR(200),
                    VALUE_COL2   NVARCHAR(500),
                    VALUE_COL3   DECIMAL(10,2),
                    RANDOM_DATA  NVARCHAR(1000),
                    STATUS       NVARCHAR(20)    DEFAULT 'ACTIVE',
                    CREATED_AT   DATETIME2       DEFAULT GETDATE(),
                    UPDATED_AT   DATETIME2       DEFAULT GETDATE(),
                    CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID)
                )
            """)
             cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT)")

        cursor.close()
        # conn.commit() # Auto-commit is usually on by default in JDBC, but explicit commit is safer if supported
        try:
             conn.commit()
        except:
             pass
        conn.close()
        logger.info("Schema setup completed successfully.")
        
    except Exception as e:
        logger.error(f"Schema setup failed: {e}")
        if conn:
            try:
                conn.close()
            except:
                pass
        sys.exit(1)

class LoadTestWorker:
    """부하 테스트 워커"""
    def __init__(self, worker_id: int, pool: JdbcConnectionPool, db_type: str, end_time: datetime):
        self.worker_id = worker_id
        self.pool = pool
        self.db_type = db_type
        self.end_time = end_time
        self.thread_name = f"Worker-{worker_id:03d}"
        self.transaction_count = 0
        
    def generate_random_data(self, length: int = 100) -> str:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def execute_transaction(self, conn) -> bool:
        cursor = None
        try:
            cursor = conn.cursor()
            
            # 1. INSERT
            # JDBC는 ? 플레이스홀더 사용
            insert_sql = """
                INSERT INTO LOAD_TEST (
                    THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                    RANDOM_DATA, STATUS, CREATED_AT
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            
            # SQL Server는 CURRENT_TIMESTAMP 대신 GETDATE() 사용 권장되나, 표준 SQL도 지원함.
            # 호환성을 위해 DB별 쿼리 조정
            if self.db_type == 'sqlserver':
                 insert_sql = """
                    INSERT INTO LOAD_TEST (
                        THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                        RANDOM_DATA, STATUS, CREATED_AT
                    ) VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                """
            elif self.db_type == 'oracle' or self.db_type == 'tibero':
                 insert_sql = """
                    INSERT INTO LOAD_TEST (
                        ID, THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                        RANDOM_DATA, STATUS, CREATED_AT
                    ) VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, SYSTIMESTAMP)
                """

            val1 = f'Value1_{self.transaction_count}'
            val2 = f'TestData_{self.generate_random_data(20)}'
            val3 = random.uniform(100, 10000)
            random_data = self.generate_random_data(200)
            status = 'ACTIVE'
            
            cursor.execute(insert_sql, (self.thread_name, val1, val2, val3, random_data, status))
            
            # Explicit commit
            conn.commit()
            
            perf_counter.increment_insert()
            
            # 2. SELECT ID (Verification)
            # JDBC에서 RETURNING을 일반화하기 어려우므로, Unique Key(ThreadID + Val1)로 조회
            select_sql = "SELECT ID, THREAD_ID FROM LOAD_TEST WHERE THREAD_ID = ? AND VALUE_COL1 = ?"
            
            cursor.execute(select_sql, (self.thread_name, val1))
            row = cursor.fetchone()
            
            perf_counter.increment_select()
            
            if row is None:
                logger.error(f"[{self.thread_name}] Verification failed: Row not found after insert")
                perf_counter.increment_verification_failure()
                return False
            
            fetched_id = row[0]
            fetched_thread_id = row[1]
            
            if fetched_thread_id != self.thread_name:
                logger.error(f"[{self.thread_name}] Data mismatch: {fetched_thread_id} != {self.thread_name}")
                perf_counter.increment_verification_failure()
                return False

            if self.transaction_count % 100 == 0:
                logger.debug(f"[{self.thread_name}] Completed {self.transaction_count} transactions")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.thread_name}] Error: {e}")
            perf_counter.increment_error()
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            # Always increment transaction count to avoid duplicate key errors on retry
            self.transaction_count += 1
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def run(self):
        logger.info(f"[{self.thread_name}] Starting worker")
        
        while datetime.now() < self.end_time:
            conn = None
            try:
                conn = self.pool.get_connection()
                if conn:
                    self.execute_transaction(conn)
            except Exception as e:
                logger.error(f"[{self.thread_name}] Connection error: {e}")
                time.sleep(1)
            finally:
                if conn:
                    self.pool.return_connection(conn)
                    
        logger.info(f"[{self.thread_name}] Completed. Transactions: {self.transaction_count}")
        return self.transaction_count

class MonitorThread(threading.Thread):
    def __init__(self, interval_seconds: int, end_time: datetime):
        super().__init__(name="Monitor", daemon=True)
        self.interval_seconds = interval_seconds
        self.end_time = end_time
        self.running = True
        
    def run(self):
        logger.info("[Monitor] Starting performance monitor")
        while self.running and datetime.now() < self.end_time:
            time.sleep(self.interval_seconds)
            stats = perf_counter.get_stats()
            logger.info(
                f"[Monitor] TPS: {stats['tps']:.2f} | "
                f"Inserts: {stats['total_inserts']:,} | "
                f"Selects: {stats['total_selects']:,} | "
                f"Errors: {stats['total_errors']:,}"
            )
    
    def stop(self):
        self.running = False

def get_jdbc_driver_info(db_type: str, base_dir: str):
    """DB 타입별 드라이버 정보 반환"""
    jre_dir = os.path.join(base_dir, 'jre')
    
    drivers = {
        'oracle': {
            'class': 'oracle.jdbc.OracleDriver',
            'jar_pattern': 'oracle/ojdbc10.jar' # Explicitly requested
        },
        'tibero': {
            'class': 'com.tmax.tibero.jdbc.TbDriver',
            'jar_pattern': 'tibero/tibero7-jdbc.jar' # Explicitly requested
        },
        'mysql': {
            'class': 'com.mysql.cj.jdbc.Driver',
            'jar_pattern': 'mysql/mysql*.jar'
        },
        'postgresql': {
            'class': 'org.postgresql.Driver',
            'jar_pattern': 'postgresql/postgresql*.jar'
        },
        'sqlserver': {
            'class': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'jar_pattern': 'sqlserver/mssql*.jar'
        }
    }
    
    if db_type not in drivers:
        raise ValueError(f"Unsupported DB type: {db_type}")
        
    info = drivers[db_type]
    
    # Jar 파일 찾기
    import glob
    pattern = os.path.join(jre_dir, info['jar_pattern'])
    jars = glob.glob(pattern)
    
    # Fallback for patterns if exact match fails (except for requested ones where we want to be strict, but let's be flexible if user made a typo in request vs reality)
    if not jars and '*' not in info['jar_pattern']:
         # Try wildcard if exact failed
         fallback_pattern = os.path.join(jre_dir, os.path.dirname(info['jar_pattern']), '*.jar')
         jars = glob.glob(fallback_pattern)

    if not jars:
        raise FileNotFoundError(f"No JDBC driver found for {db_type} in {pattern}")
        
    # 첫 번째 발견된 jar 사용
    return info['class'], jars[0]

def main():
    parser = argparse.ArgumentParser(description='Multi-DB JDBC Load Tester')
    parser.add_argument('--db-type', required=True, choices=['oracle', 'tibero', 'mysql', 'postgresql', 'sqlserver'])
    parser.add_argument('--db-url', required=True, help='JDBC URL')
    parser.add_argument('--db-user', required=True, help='DB User')
    parser.add_argument('--db-password', required=True, help='DB Password')
    parser.add_argument('--thread-count', type=int, default=10)
    parser.add_argument('--test-duration', type=int, default=60)
    parser.add_argument('--min-pool', type=int, default=5)
    parser.add_argument('--max-pool', type=int, default=20)
    
    args = parser.parse_args()
    
    # 드라이버 정보 가져오기
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        driver_class, jar_path = get_jdbc_driver_info(args.db_type, base_dir)
        logger.info(f"Using Driver: {driver_class}")
        logger.info(f"Using Jar: {jar_path}")
    except Exception as e:
        logger.error(str(e))
        sys.exit(1)
        
    # JVM 초기화
    init_jvm([jar_path])
    
    # 설정 객체
    config = JdbcConfig(
        db_type=args.db_type,
        url=args.db_url,
        user=args.db_user,
        password=args.db_password,
        driver_class=driver_class,
        jar_path=jar_path,
        min_pool_size=args.min_pool,
        max_pool_size=args.max_pool
    )
    
    # 스키마 설정 (Drop & Create)
    setup_schema(config)
    
    # 연결 풀 생성
    pool = JdbcConnectionPool(config)
    
    # 테스트 실행
    end_time = datetime.now() + timedelta(seconds=args.test_duration)
    monitor = MonitorThread(5, end_time)
    monitor.start()
    
    logger.info(f"Starting load test with {args.thread_count} threads for {args.test_duration}s")
    
    with ThreadPoolExecutor(max_workers=args.thread_count) as executor:
        futures = []
        for i in range(args.thread_count):
            worker = LoadTestWorker(i+1, pool, args.db_type, end_time)
            futures.append(executor.submit(worker.run))
            
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Worker failed: {e}")
                
    monitor.stop()
    monitor.join()
    
    # 풀 정리
    pool.close_all()
    
    # 최종 통계
    stats = perf_counter.get_stats()
    logger.info("="*50)
    logger.info("TEST COMPLETED")
    logger.info(f"Total Inserts: {stats['total_inserts']:,}")
    logger.info(f"Total Selects: {stats['total_selects']:,}")
    logger.info(f"TPS: {stats['tps']:.2f}")
    logger.info("="*50)

if __name__ == "__main__":
    main()
