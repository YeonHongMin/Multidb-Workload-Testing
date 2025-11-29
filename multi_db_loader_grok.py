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

-- 추가 인덱스 (각 파티션에 LOCAL 인덱스 자동 생성되지 않으므로, 필요 시 각 파티션에 추가)
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT);

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

SQL Server DDL:
-- 1. HASH PARTITION 함수 및 스키마 생성 (SQL Server 2005+)
CREATE PARTITION FUNCTION PF_LOAD_TEST (BIGINT) 
AS RANGE RIGHT FOR VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);  -- HASH 시뮬레이션 위해 범위 사용 (실제 HASH는 FILEGROUP 필요)

CREATE PARTITION SCHEME PS_LOAD_TEST 
AS PARTITION PF_LOAD_TEST 
TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]);  -- 16개 그룹

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
) ON PS_LOAD_TEST(ID);  -- 파티션 적용

-- PRIMARY KEY
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);

-- 추가 인덱스
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT);
CREATE INDEX IDX_LOAD_TEST_CREATED ON LOAD_TEST(CREATED_AT);

Tibero DDL (Oracle 호환, 기존 Oracle DDL 사용 가능):
-- Oracle DDL과 동일하게 사용 가능 (Tibero는 Oracle 호환성 높음)

[2] Python 소스코드 (multi_db_load_tester.py) - SQLAlchemy를 사용한 다중 DB 지원 버전

"""
#!/usr/bin/env python3
"""
Multi-DB Load Tester - MySQL, PostgreSQL, Oracle, SQL Server, Tibero 지원
SQLAlchemy를 사용한 추상화된 부하 테스트 도구

[3] requirements.txt
sqlalchemy>=2.0.0
python-dotenv>=1.0.0
# DB별 드라이버 (설치 필요):
# MySQL: pip install pymysql
# PostgreSQL: pip install psycopg2-binary
# Oracle: pip install oracledb
# SQL Server: pip install pyodbc (ODBC 드라이버 설치 필요)
# Tibero: pip install tibpy (또는 oracledb 호환)

[4] 환경 설정 파일 (선택사항: .env)
# .env 파일 예시
DB_URL=mysql+pymysql://user:password@host:port/dbname  # MySQL 예시
# 또는 postgresql://user:password@host:port/dbname
# 또는 oracle+oracledb://user:password@dsn
# 또는 mssql+pyodbc://user:password@server/dbname?driver=ODBC+Driver+17+for+SQL+Server
# 또는 tibero://user:password@host:port/dbname (Tibero 호환)
DB_TYPE=mysql  # mysql, postgresql, oracle, sqlserver, tibero
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200
THREAD_COUNT=150
TEST_DURATION_SECONDS=600

[5] 실행 방법
환경 준비 (기존과 동일, requirements.txt 업데이트)

[6] 데이터베이스 준비
# 각 DB별 DDL 실행 (위 DDL 사용)

[7] 부하 테스트 실행 예시
# MySQL 예시
python multi_db_load_tester.py \
    --db-url="mysql+pymysql://TEST_USER:TestPassword123@localhost:3306/testdb" \
    --db-type=mysql \
    --thread-count=100 \
    --test-duration-seconds=300

# PostgreSQL 예시
python multi_db_load_tester.py \
    --db-url="postgresql://TEST_USER:TestPassword123@localhost:5432/testdb" \
    --db-type=postgresql \
    --thread-count=100 \
    --test-duration-seconds=300

# Oracle 예시 (기존과 유사)
python multi_db_load_tester.py \
    --db-url="oracle+oracledb://TEST_USER:TestPassword123@localhost:1521/XEPDB1" \
    --db-type=oracle \
    --thread-count=100 \
    --test-duration-seconds=300

# SQL Server 예시
python multi_db_load_tester.py \
    --db-url="mssql+pyodbc://TEST_USER:TestPassword123@server/testdb?driver=ODBC+Driver+17+for+SQL+Server" \
    --db-type=sqlserver \
    --thread-count=100 \
    --test-duration-seconds=300

# Tibero 예시 (Oracle 호환)
python multi_db_load_tester.py \
    --db-url="tibero://TEST_USER:TestPassword123@localhost:8629/tibero" \
    --db-type=tibero \
    --thread-count=100 \
    --test-duration-seconds=300

[8] 추가 모니터링 SQL (DB별 약간 다름, 예: MySQL)
-- 실시간 세션: SHOW PROCESSLIST WHERE User = 'TEST_USER';
-- 테이블 통계: SELECT COUNT(*) FROM LOAD_TEST; 등 (기존과 유사, DB별 조정)

[9] 주요 특징
. 다중 DB 지원: SQLAlchemy를 통한 추상화 (INSERT/SELECT 통합)
. DB 타입별 SQL 조정: SEQUENCE/AUTO_INCREMENT/IDENTITY 처리
. 연결 풀: SQLAlchemy의 QueuePool 사용
. 에러 복구: 재연결 로직 유지
. 실시간 모니터링: 기존과 동일
. 스레드 안전성: 기존과 동일
이 프로그램은 각 RDBMS의 부하 처리 능력을 테스트합니다.
"""

import sqlalchemy as sa
from sqlalchemy import create_engine, text, event
from sqlalchemy.pool import QueuePool
import threading
import logging
import time
import random
import string
import argparse
import sys
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# 로깅 설정 (기존과 동일)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)-12s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_db_load_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 성능 카운터 (기존과 동일)
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

# 전역 성능 카운터
perf_counter = PerformanceCounter()

@dataclass
class DatabaseConfig:
    """데이터베이스 연결 설정"""
    url: str
    db_type: str  # mysql, postgresql, oracle, sqlserver, tibero
    min_pool_size: int = 100
    max_pool_size: int = 200
    pool_pre_ping: bool = True  # 연결 검증

def get_insert_sql(db_type: str) -> str:
    """DB 타입에 따른 INSERT SQL 반환"""
    if db_type in ['oracle', 'tibero']:
        return """
            INSERT INTO LOAD_TEST (
                THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                RANDOM_DATA, STATUS, CREATED_AT
            ) VALUES (
                :thread_id, :val1, :val2, :val3,
                :random_data, :status, CURRENT_TIMESTAMP
            ) RETURNING ID INTO :new_id
        """
    elif db_type == 'postgresql':
        return """
            INSERT INTO LOAD_TEST (
                THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                RANDOM_DATA, STATUS, CREATED_AT
            ) VALUES (
                :thread_id, :val1, :val2, :val3,
                :random_data, :status, CURRENT_TIMESTAMP
            ) RETURNING ID
        """
    elif db_type == 'mysql':
        return """
            INSERT INTO LOAD_TEST (
                THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                RANDOM_DATA, STATUS, CREATED_AT
            ) VALUES (
                %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
            )
        """  # MySQL은 %s 플레이스홀더, RETURNING 없음 -> LAST_INSERT_ID() 별도
    elif db_type == 'sqlserver':
        return """
            INSERT INTO LOAD_TEST (
                THREAD_ID, VALUE_COL1, VALUE_COL2, VALUE_COL3, 
                RANDOM_DATA, STATUS, CREATED_AT
            ) VALUES (
                :thread_id, :val1, :val2, :val3,
                :random_data, :status, GETDATE()
            );
            SELECT SCOPE_IDENTITY() AS new_id
        """
    else:
        raise ValueError(f"Unsupported DB type: {db_type}")

def get_select_sql(db_type: str) -> str:
    """DB 타입에 따른 SELECT SQL 반환 (모두 동일)"""
    return """
        SELECT ID, THREAD_ID, VALUE_COL1, STATUS 
        FROM LOAD_TEST 
        WHERE ID = :id
    """

class LoadTestWorker:
    """부하 테스트 워커 클래스"""
    
    def __init__(self, worker_id: int, engine: sa.Engine, db_type: str, end_time: datetime):
        self.worker_id = worker_id
        self.engine = engine
        self.db_type = db_type
        self.end_time = end_time
        self.thread_name = f"Worker-{worker_id:03d}"
        self.transaction_count = 0
        
    def generate_random_data(self, length: int = 100) -> str:
        """랜덤 데이터 생성 (기존과 동일)"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def execute_transaction(self, connection: sa.Connection) -> bool:
        """단일 트랜잭션 실행 (INSERT -> COMMIT -> SELECT -> VERIFY)"""
        try:
            insert_sql = text(get_insert_sql(self.db_type))
            select_sql = text(get_select_sql(self.db_type))
            
            with connection.begin():  # 트랜잭션 시작
                # 1. INSERT
                data = {
                    'thread_id': self.thread_name,
                    'val1': f'Value1_{self.transaction_count}',
                    'val2': f'TestData_{self.generate_random_data(20)}',
                    'val3': random.uniform(100, 10000),
                    'random_data': self.generate_random_data(200),
                    'status': 'ACTIVE'
                }
                
                if self.db_type == 'mysql':
                    # MySQL: %s 플레이스홀더, execute 후 lastrowid
                    result = connection.execute(insert_sql, [
                        data['thread_id'], data['val1'], data['val2'], data['val3'],
                        data['random_data'], data['status']
                    ])
                    new_id = connection.scalar(text("SELECT LAST_INSERT_ID()"))
                elif self.db_type in ['oracle', 'tibero']:
                    # Oracle/Tibero: RETURNING INTO (SQLAlchemy out 파라미터)
                    new_id = connection.execute(insert_sql, data).scalar()
                elif self.db_type == 'postgresql':
                    # PostgreSQL: RETURNING
                    result = connection.execute(insert_sql, data)
                    new_id = result.scalar()
                elif self.db_type == 'sqlserver':
                    # SQL Server: SCOPE_IDENTITY()
                    result = connection.execute(insert_sql, data)
                    new_id = result.scalar()
                else:
                    raise ValueError(f"Unsupported DB type: {self.db_type}")
                
                if new_id is None:
                    raise ValueError("Failed to retrieve new ID")
                
                perf_counter.increment_insert()
                
                # 2. SELECT로 검증
                row = connection.execute(select_sql, {'id': new_id}).fetchone()
                perf_counter.increment_select()
                
                # 3. 검증
                if row is None:
                    logger.error(f"[{self.thread_name}] Verification failed: Row with ID={new_id} not found")
                    perf_counter.increment_verification_failure()
                    return False
                
                if row[0] != new_id or row[1] != self.thread_name:
                    logger.error(f"[{self.thread_name}] Data mismatch for ID={new_id}")
                    perf_counter.increment_verification_failure()
                    return False
                
                self.transaction_count += 1
                
                # 주기적 로그 (기존과 동일)
                if self.transaction_count % 100 == 0:
                    logger.debug(f"[{self.thread_name}] Completed {self.transaction_count} transactions")
                
                return True
                
        except Exception as e:
            logger.error(f"[{self.thread_name}] Error in transaction: {str(e)}")
            perf_counter.increment_error()
            return False
    
    def run(self):
        """워커 실행 메인 루프"""
        logger.info(f"[{self.thread_name}] Starting worker thread")
        
        while datetime.now() < self.end_time:
            connection = None
            try:
                # 연결 획득 (SQLAlchemy는 자동 풀 관리)
                with self.engine.connect() as connection:
                    connection.execution_options(isolation_level="AUTOCOMMIT")  # 필요 시
                    success = self.execute_transaction(connection)
                
                # 실행 간 짧은 대기 (선택)
                # time.sleep(random.uniform(0.001, 0.01))
                
            except Exception as e:
                logger.error(f"[{self.thread_name}] Connection error: {str(e)}")
                time.sleep(0.5)  # 재시도 대기
        
        logger.info(f"[{self.thread_name}] Worker completed. Total transactions: {self.transaction_count}")
        return self.transaction_count

class MonitorThread(threading.Thread):
    """모니터링 스레드 (기존과 동일)"""
    
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
                f"[Monitor] Stats - "
                f"Inserts: {stats['total_inserts']:,} | "
                f"Selects: {stats['total_selects']:,} | "
                f"Errors: {stats['total_errors']:,} | "
                f"Verification Failures: {stats['verification_failures']:,} | "
                f"TPS: {stats['tps']:.2f} | "
                f"Elapsed: {stats['elapsed_seconds']:.1f}s"
            )
        
        logger.info("[Monitor] Stopping performance monitor")
    
    def stop(self):
        self.running = False

class MultiDBLoadTester:
    """다중 DB 부하 테스트 메인 클래스"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine: Optional[sa.Engine] = None
        
    def create_engine(self) -> sa.Engine:
        """엔진 생성 (SQLAlchemy)"""
        logger.info(f"Creating SQLAlchemy engine for {self.config.db_type} (pool min={self.config.min_pool_size}, max={self.config.max_pool_size})")
        
        pool_args = {
            'poolclass': QueuePool,
            'pool_pre_ping': self.config.pool_pre_ping,
            'pool_size': self.config.max_pool_size,
            'max_overflow': self.config.max_pool_size - self.config.min_pool_size,
            'pool_recycle': 3600,  # 1시간마다 재사용
            'pool_reset_on_return': 'rollback'
        }
        
        engine = create_engine(
            self.config.url,
            pool_args,
            echo=False  # 디버그 시 True
        )
        
        # DB 타입별 이벤트 핸들러 (예: 세션 콜백)
        if self.config.db_type in ['oracle', 'tibero']:
            @event.listens_for(engine, "connect")
            def set_oracle_session(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
                cursor.close()
        
        logger.info("Engine created successfully")
        return engine
    
    def run_load_test(self, thread_count: int, duration_seconds: int):
        """부하 테스트 실행 (기존과 유사, engine 사용)"""
        logger.info(f"Starting load test: {thread_count} threads for {duration_seconds} seconds on {self.config.db_type}")
        
        # 엔진 생성
        self.engine = self.create_engine()
        
        # 종료 시간
        end_time = datetime.now() + timedelta(seconds=duration_seconds)
        
        # 모니터링 시작
        monitor = MonitorThread(interval_seconds=5, end_time=end_time)
        monitor.start()
        
        # 워커 실행
        total_transactions = 0
        with ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="Worker") as executor:
            futures = []
            for i in range(thread_count):
                worker = LoadTestWorker(i + 1, self.engine, self.config.db_type, end_time)
                future = executor.submit(worker.run)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    total_transactions += result
                except Exception as e:
                    logger.error(f"Worker thread failed: {str(e)}")
        
        # 모니터링 정지
        monitor.stop()
        monitor.join(timeout=5)
        
        # 최종 통계 (기존과 동일)
        final_stats = perf_counter.get_stats()
        logger.info("="*80)
        logger.info("LOAD TEST COMPLETED - FINAL STATISTICS")
        logger.info("="*80)
        logger.info(f"DB Type: {self.config.db_type}")
        logger.info(f"Total Threads: {thread_count}")
        logger.info(f"Test Duration: {duration_seconds} seconds")
        logger.info(f"Total Inserts: {final_stats['total_inserts']:,}")
        logger.info(f"Total Selects: {final_stats['total_selects']:,}")
        logger.info(f"Total Errors: {final_stats['total_errors']:,}")
        logger.info(f"Verification Failures: {final_stats['verification_failures']:,}")
        logger.info(f"Average TPS: {final_stats['tps']:.2f}")
        logger.info(f"Total Transactions per Thread: {total_transactions / thread_count:.2f}")
        logger.info("="*80)
        
        # 엔진 정리
        if self.engine:
            self.engine.dispose()
            logger.info("Engine disposed")

def parse_arguments():
    """명령행 인자 파싱 (DB URL과 타입 추가)"""
    parser = argparse.ArgumentParser(
        description='Multi-DB Load Tester',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # 데이터베이스 연결 정보
    parser.add_argument(
        '--db-url',
        required=True,
        help='SQLAlchemy DB URL (e.g., mysql+pymysql://...)'
    )
    parser.add_argument(
        '--db-type',
        choices=['mysql', 'postgresql', 'oracle', 'sqlserver', 'tibero'],
        required=True,
        help='Database type'
    )
    
    # 풀 설정 (기존)
    parser.add_argument(
        '--min-pool-size',
        type=int,
        default=100,
        help='Minimum pool size'
    )
    parser.add_argument(
        '--max-pool-size',
        type=int,
        default=200,
        help='Maximum pool size'
    )
    
    # 테스트 설정 (기존)
    parser.add_argument(
        '--thread-count',
        type=int,
        default=100,
        help='Number of worker threads'
    )
    parser.add_argument(
        '--test-duration-seconds',
        type=int,
        default=300,
        help='Test duration in seconds'
    )
    
    # 로깅 (기존)
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )
    
    return parser.parse_args()

def main():
    """메인 함수 (기존과 유사)"""
    args = parse_arguments()
    
    # 로깅 레벨
    logger.setLevel(getattr(logging, args.log_level))
    
    # 설정 출력
    logger.info("="*80)
    logger.info("MULTI-DB LOAD TESTER CONFIGURATION")
    logger.info("="*80)
    logger.info(f"DB Type: {args.db_type}")
    logger.info(f"DB URL: {args.db_url[:50]}...")
    logger.info(f"Min Pool Size: {args.min_pool_size}")
    logger.info(f"Max Pool Size: {args.max_pool_size}")
    logger.info(f"Thread Count: {args.thread_count}")
    logger.info(f"Test Duration: {args.test_duration_seconds} seconds")
    logger.info("="*80)
    
    # 설정 생성
    db_config = DatabaseConfig(
        url=args.db_url,
        db_type=args.db_type,
        min_pool_size=args.min_pool_size,
        max_pool_size=args.max_pool_size
    )
    
    # 실행
    try:
        tester = MultiDBLoadTester(db_config)
        tester.run_load_test(
            thread_count=args.thread_count,
            duration_seconds=args.test_duration_seconds
        )
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

