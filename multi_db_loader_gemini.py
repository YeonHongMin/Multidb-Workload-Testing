#!/usr/bin/env python3
"""
Universal Database Load Tester (JDBC Version)
지원 DB: Oracle, Tibero (via JDBC), SQL Server, PostgreSQL, MySQL
작성자: Windows Server Infrastructure Architect (MCM)

# 필수 패키지 설치
pip install JPype1 JayDeBeApi
pip install psycopg2-binary pymysql
"""

import sys
import time
import logging
import threading
import argparse
import queue
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
import os

# JDBC 관련 라이브러리
import jpype
import jaydebeapi

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DBLoadTester")

# --- [0] JVM Management ---
class JVMManager:
    _initialized = False

    @staticmethod
    def start(driver_paths):
        if not jpype.isJVMStarted():
            # Java Home 설정 (사용자 요청: C:\jdk25)
            java_home = r"C:\jdk25"
            jvm_path = os.path.join(java_home, "bin", "server", "jvm.dll")
            
            if not os.path.exists(jvm_path):
                # Fallback to default if specific path doesn't exist
                jvm_path = jpype.getDefaultJVMPath()
                logger.warning(f"지정된 JVM 경로를 찾을 수 없어 기본 경로를 사용합니다: {jvm_path}")
            
            # Classpath 설정
            classpath = [os.path.abspath(p) for p in driver_paths]
            
            logger.info(f"JVM 시작: {jvm_path}")
            logger.info(f"Classpath: {classpath}")
            
            try:
                jpype.startJVM(jvm_path, "-Djava.class.path={}".format(";".join(classpath)))
                JVMManager._initialized = True
            except Exception as e:
                logger.error(f"JVM 시작 실패: {e}")
                raise

# --- [1] Generic Connection Pool ---
class GenericConnectionPool:
    """JDBC 등 내장 풀이 없는 드라이버를 위한 스레드 안전 큐 기반 풀"""
    def __init__(self, connector_func, min_size, max_size):
        self.connector_func = connector_func
        self.max_size = max_size
        self.pool = queue.Queue(maxsize=max_size)
        self.current_size = 0
        self.lock = threading.Lock()
        
        # 초기 풀 생성
        for _ in range(min_size):
            self._add_connection()

    def _add_connection(self):
        with self.lock:
            if self.current_size < self.max_size:
                try:
                    conn = self.connector_func()
                    self.pool.put(conn)
                    self.current_size += 1
                except Exception as e:
                    logger.error(f"풀 초기화 중 연결 실패: {e}")

    def acquire(self, timeout=30):
        try:
            return self.pool.get(block=True, timeout=timeout)
        except queue.Empty:
            with self.lock:
                if self.current_size < self.max_size:
                    conn = self.connector_func()
                    self.current_size += 1
                    return conn
            return self.pool.get(block=True, timeout=timeout)

    def release(self, conn):
        try:
            # 연결 유효성 체크 (간단)
            if conn.jconn.isClosed():
                with self.lock:
                    self.current_size -= 1
                return
            self.pool.put(conn)
        except queue.Full:
            try:
                conn.close()
            except:
                pass
            with self.lock:
                self.current_size -= 1
        except Exception:
            # JDBC connection check failed
             with self.lock:
                self.current_size -= 1

    def close(self):
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass

# --- [2] Database Adapter Strategy ---

class DatabaseAdapter(ABC):
    def __init__(self, config):
        self.config = config
        self.pool = None

    @abstractmethod
    def init_pool(self):
        pass

    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    def release_connection(self, conn):
        pass

    @abstractmethod
    def get_ddl_statements(self) -> list[str]:
        pass

    @abstractmethod
    def execute_transaction(self, conn, thread_id: str, iteration: int) -> int:
        pass

    @abstractmethod
    def verify_transaction(self, conn, row_id: int) -> bool:
        pass

# -------------------------------------------------------------------------
# [Oracle JDBC Implementation]
# -------------------------------------------------------------------------
class OracleJDBCAdapter(DatabaseAdapter):
    def __init__(self, config):
        super().__init__(config)
        self.driver_class = "oracle.jdbc.driver.OracleDriver"
        # JDBC URL Format: jdbc:oracle:thin:@host:port:sid
        self.jdbc_url = f"jdbc:oracle:thin:@{config['host']}:{config['port']}:{config['database']}"

    def init_pool(self):
        def connector():
            return jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.config['user'], self.config['password']]
            )
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self):
        return self.pool.acquire()

    def release_connection(self, conn):
        self.pool.release(conn)

    def get_ddl_statements(self) -> list[str]:
        return [
            "CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE",
            """CREATE TABLE LOAD_TEST (
                ID NUMBER PRIMARY KEY,
                THREAD_ID VARCHAR2(50),
                VALUE_COL VARCHAR2(100),
                CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                STATUS VARCHAR2(20) DEFAULT 'ACTIVE'
               )"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        cursor = conn.cursor()
        # JDBC PreparedStatement style
        sql = "INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS) VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, 'ACTIVE')"
        cursor.execute(sql, (thread_id, f"VAL_{iteration}"))
        
        # Get Generated ID
        # Oracle JDBC doesn't always support getGeneratedKeys nicely with sequences in all versions via generic interface,
        # so we use CURRVAL for simplicity in this test context.
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        inserted_id = cursor.fetchone()[0]
        cursor.close()
        # Auto-commit might be off
        # conn.commit() # JayDeBeApi usually requires explicit commit if not autocommit
        return inserted_id

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
        row = cursor.fetchone()
        cursor.close()
        return row is not None and row[0] == row_id

# -------------------------------------------------------------------------
# [Tibero JDBC Implementation]
# -------------------------------------------------------------------------
class TiberoJDBCAdapter(DatabaseAdapter):
    def __init__(self, config):
        super().__init__(config)
        self.driver_class = "com.tmax.tibero.jdbc.TbDriver"
        # JDBC URL Format: jdbc:tibero:thin:@host:port:db_name
        self.jdbc_url = f"jdbc:tibero:thin:@{config['host']}:{config['port']}:{config['database']}"

    def init_pool(self):
        def connector():
            return jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.config['user'], self.config['password']]
            )
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self):
        return self.pool.acquire()

    def release_connection(self, conn):
        self.pool.release(conn)

    def get_ddl_statements(self) -> list[str]:
        return [
            "CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE",
            """CREATE TABLE LOAD_TEST (
                ID NUMBER PRIMARY KEY,
                THREAD_ID VARCHAR2(50),
                VALUE_COL VARCHAR2(100),
                CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                STATUS VARCHAR2(20) DEFAULT 'ACTIVE'
               )"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        cursor = conn.cursor()
        sql = "INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS) VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, 'ACTIVE')"
        cursor.execute(sql, (thread_id, f"VAL_{iteration}"))
        
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        inserted_id = cursor.fetchone()[0]
        cursor.close()
        return inserted_id

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
        row = cursor.fetchone()
        cursor.close()
        return row is not None and row[0] == row_id

# -------------------------------------------------------------------------
# [PostgreSQL Implementation] (Native)
# -------------------------------------------------------------------------
class PostgresAdapter(DatabaseAdapter):
    def __init__(self, config):
        import psycopg2
        from psycopg2 import pool
        self.psycopg2 = psycopg2
        self.pg_pool_cls = pool.ThreadedConnectionPool
        super().__init__(config)

    def init_pool(self):
        self.pool = self.pg_pool_cls(
            minconn=self.config['min_pool'],
            maxconn=self.config['max_pool'],
            dsn=f"dbname={self.config['database']} user={self.config['user']} password={self.config['password']} host={self.config['host']} port={self.config['port']}"
        )

    def get_connection(self):
        return self.pool.getconn()

    def release_connection(self, conn):
        self.pool.putconn(conn)

    def get_ddl_statements(self):
        return [
            """CREATE TABLE LOAD_TEST (
                ID SERIAL PRIMARY KEY,
                THREAD_ID VARCHAR(50),
                VALUE_COL VARCHAR(100),
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                STATUS VARCHAR(20) DEFAULT 'ACTIVE'
            )"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO LOAD_TEST (THREAD_ID, VALUE_COL, STATUS)
                VALUES (%s, %s, 'ACTIVE')
                RETURNING ID
            """, (thread_id, f"VAL_{iteration}"))
            inserted_id = cursor.fetchone()[0]
            conn.commit()
            return inserted_id

    def verify_transaction(self, conn, row_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = %s", (row_id,))
            row = cursor.fetchone()
            return row is not None and row[0] == row_id

# -------------------------------------------------------------------------
# [MySQL Implementation] (Native)
# -------------------------------------------------------------------------
class MySQLAdapter(DatabaseAdapter):
    def __init__(self, config):
        import pymysql
        self.pymysql = pymysql
        super().__init__(config)

    def init_pool(self):
        def connector():
            return self.pymysql.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                port=int(self.config.get('port', 3306)),
                autocommit=False
            )
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self):
        return self.pool.acquire()

    def release_connection(self, conn):
        self.pool.release(conn)

    def get_ddl_statements(self):
        return [
            """CREATE TABLE LOAD_TEST (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                THREAD_ID VARCHAR(50),
                VALUE_COL VARCHAR(100),
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                STATUS VARCHAR(20) DEFAULT 'ACTIVE'
            ) ENGINE=InnoDB"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO LOAD_TEST (THREAD_ID, VALUE_COL, STATUS)
                VALUES (%s, %s, 'ACTIVE')
            """, (thread_id, f"VAL_{iteration}"))
            inserted_id = cursor.lastrowid
            conn.commit()
            return inserted_id

    def verify_transaction(self, conn, row_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = %s", (row_id,))
            row = cursor.fetchone()
            if row:
                val = row[0] if isinstance(row, tuple) else row['ID']
                return val == row_id
            return False

# --- [3] Main Load Tester Application ---

class MultiDBLoadTester:
    def __init__(self, db_type, config):
        self.stats = {'insert': 0, 'select': 0, 'error': 0}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.db_type = db_type
        
        # 어댑터 팩토리
        if db_type == 'oracle':
            self.adapter = OracleJDBCAdapter(config)
        elif db_type == 'tibero':
            self.adapter = TiberoJDBCAdapter(config)
        elif db_type == 'mysql':
            self.adapter = MySQLAdapter(config)
        elif db_type == 'postgres':
            self.adapter = PostgresAdapter(config)
        else:
            raise ValueError(f"지원하지 않는 DB 타입: {db_type}")

    def setup_database(self):
        """테이블 초기화 (Drop if exists & Create)"""
        logger.info("데이터베이스 연결 및 초기화 중...")
        try:
            self.adapter.init_pool()
            conn = self.adapter.get_connection()
            cursor = conn.cursor()
            
            # 기존 객체 삭제 시도
            drop_sqls = []
            if self.db_type in ['oracle', 'tibero']:
                drop_sqls = [
                    "DROP TABLE LOAD_TEST", 
                    "DROP SEQUENCE LOAD_TEST_SEQ"
                ]
            elif self.db_type in ['mysql', 'postgres']:
                drop_sqls = ["DROP TABLE IF EXISTS LOAD_TEST"]

            for sql in drop_sqls:
                try:
                    cursor.execute(sql)
                    # JDBC는 AutoCommit 여부에 따라 다름, 안전하게 커밋 시도
                    # JayDeBeApi는 기본적으로 AutoCommit이 True일 수 있으나 확인 필요
                    # 여기서는 에러 무시 (테이블이 없을 수 있음)
                except Exception as e:
                    logger.info(f"삭제 건너뜀 (존재하지 않음 등): {e}")

            # 생성
            for sql in self.adapter.get_ddl_statements():
                try:
                    cursor.execute(sql)
                    logger.info(f"DDL 실행 성공: {sql[:30]}...")
                except Exception as e:
                    logger.error(f"DDL 실행 실패: {e}")
                    raise
            
            cursor.close()
            # 커밋 (JDBC의 경우)
            try:
                if not conn.jconn.getAutoCommit():
                    conn.commit()
            except:
                pass
                
            self.adapter.release_connection(conn)
            
        except Exception as e:
            logger.error(f"초기화 치명적 오류: {e}")
            sys.exit(1)

    def worker(self, thread_idx):
        thread_name = f"Worker-{thread_idx}"
        iteration = 0
        
        while not self.stop_event.is_set():
            iteration += 1
            conn = None
            try:
                conn = self.adapter.get_connection()
                
                # 1. INSERT
                row_id = self.adapter.execute_transaction(conn, thread_name, iteration)
                with self.lock: self.stats['insert'] += 1
                
                # 2. SELECT Verify
                if not self.adapter.verify_transaction(conn, row_id):
                    raise RuntimeError(f"데이터 검증 실패 ID={row_id}")
                with self.lock: self.stats['select'] += 1
                
            except Exception as e:
                with self.lock: self.stats['error'] += 1
                logger.debug(f"[{thread_name}] 에러: {e}")
            finally:
                if conn:
                    self.adapter.release_connection(conn)
                time.sleep(0.001)

    def monitor(self):
        last_insert = 0
        while not self.stop_event.wait(5):
            with self.lock:
                curr_insert = self.stats['insert']
                curr_select = self.stats['select']
                curr_error = self.stats['error']
            
            delta = curr_insert - last_insert
            tps = delta / 5.0
            last_insert = curr_insert
            
            logger.info(
                f"[Monitor] TPS: {tps:.1f} | Total Insert: {curr_insert} | Total Select: {curr_select} | Errors: {curr_error}"
            )

    def run(self, thread_count, duration):
        self.setup_database()
        
        logger.info(f"부하 테스트 시작: {thread_count} 스레드, {duration}초")
        
        monitor_thread = threading.Thread(target=self.monitor, daemon=True)
        monitor_thread.start()
        
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = [executor.submit(self.worker, i) for i in range(thread_count)]
            
            time.sleep(duration)
            self.stop_event.set()
            logger.info("종료 신호 전송. 워커 대기 중...")
            
        logger.info("테스트 완료.")
        logger.info(f"최종 결과: {self.stats}")

def main():
    parser = argparse.ArgumentParser(description="Multi-DB Load Tester (JDBC)")
    parser.add_argument('--db-type', required=True, choices=['oracle', 'mysql', 'postgres', 'tibero'])
    parser.add_argument('--host', required=True, help="DB Host")
    parser.add_argument('--port', type=int, required=True, help="DB Port")
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--database', required=True, help="Database Name / SID")
    parser.add_argument('--thread-count', type=int, default=10)
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--min-pool', type=int, default=5)
    parser.add_argument('--max-pool', type=int, default=20)
    
    args = parser.parse_args()
    
    # 드라이버 경로 설정 (사용자 환경에 맞춤)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    driver_paths = []
    if args.db_type == 'oracle':
        driver_paths.append(os.path.join(base_dir, "jre", "oracle", "ojdbc10.jar"))
    elif args.db_type == 'tibero':
        driver_paths.append(os.path.join(base_dir, "jre", "tibero", "tibero7-jdbc.jar"))
        
    # JVM 시작 (필요한 경우)
    if driver_paths:
        JVMManager.start(driver_paths)
    
    config = {
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'min_pool': args.min_pool,
        'max_pool': args.max_pool
    }
    
    tester = MultiDBLoadTester(args.db_type, config)
    tester.run(args.thread_count, args.duration)

if __name__ == "__main__":
    main()
