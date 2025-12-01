#!/usr/bin/env python3
"""
[Universal Database Load Tester - JDBC Version]
- 지원 DB: Oracle, Tibero (JDBC), SQL Server, PostgreSQL, MySQL
- 기능: 멀티스레드 부하 테스트 (INSERT -> COMMIT -> SELECT 검증)
- 작성: Windows Server Infrastructure Architect
- 주의: 실행 시 'LOAD_TEST' 테이블이 초기화(DROP/CREATE) 됩니다.

1. Oracle (JDBC)
python multi_db_loader_gemini_etc.py --db-type oracle --db-host 192.168.0.172 --db-port 1521 --db-name DEV --user app --password app --thread-count 50

2. Tibero (JDBC)
python multi_db_loader_gemini_etc.py --db-type tibero --db-host 192.168.0.140 --db-port 8629 --db-name prod --user app --password app --thread-count 50
"""

import sys
import time
import logging
import threading
import argparse
import queue
import os
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

# JDBC imports
try:
    import jaydebeapi
    import jpype
    JDBC_AVAILABLE = True
except ImportError:
    JDBC_AVAILABLE = False
    logging.warning("jaydebeapi or jpype not available. JDBC connections will not work.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DBLoadTester")

# -------------------------------------------------------------------------
# [1] Generic Connection Pool (ODBC/MySQL/JDBC용 - 내장 풀 없는 경우)
# -------------------------------------------------------------------------
class GenericConnectionPool:
    """스레드 안전 큐(Queue)를 이용한 커스텀 커넥션 풀"""
    def __init__(self, connector_func, min_size, max_size):
        self.connector_func = connector_func
        self.max_size = max_size
        self.pool = queue.Queue(maxsize=max_size)
        self.current_size = 0
        self.lock = threading.Lock()
        
        # 초기 커넥션 생성
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
                    logger.error(f"풀 초기화 실패: {e}")

    def acquire(self, timeout=30):
        try:
            return self.pool.get(block=True, timeout=timeout)
        except queue.Empty:
            # 풀이 비어있고 Max 미만이면 생성
            with self.lock:
                if self.current_size < self.max_size:
                    conn = self.connector_func()
                    self.current_size += 1
                    return conn
            # 생성 불가 시 대기
            return self.pool.get(block=True, timeout=timeout)

    def release(self, conn):
        try:
            self.pool.put(conn) # 큐에 반환
        except queue.Full:
            try:
                conn.close() # 큐가 꽉 찼으면 종료
            except:
                pass
            with self.lock:
                self.current_size -= 1

    def close(self):
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass

# -------------------------------------------------------------------------
# [2] Database Strategy Pattern (추상 클래스 및 구현체)
# -------------------------------------------------------------------------
class DatabaseAdapter(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None

    @abstractmethod
    def init_pool(self): pass

    @abstractmethod
    def get_connection(self): pass

    @abstractmethod
    def release_connection(self, conn): pass

    @abstractmethod
    def get_ddl_statements(self) -> list[str]: pass

    @abstractmethod
    def execute_transaction(self, conn, thread_id: str, iteration: int) -> int: pass

    @abstractmethod
    def verify_transaction(self, conn, row_id: int) -> bool: pass

    @abstractmethod
    def check_and_drop_objects(self, conn): pass

# --- Oracle (JDBC) ---
class OracleAdapter(DatabaseAdapter):
    def __init__(self, config):
        super().__init__(config)
        if not JDBC_AVAILABLE:
            raise ImportError("jaydebeapi and jpype are required for Oracle JDBC connection")
        
        # JDBC 드라이버 경로 설정 (./jre/oracle/ojdbc10.jar)
        self.jar_path = os.path.join(os.path.dirname(__file__), "jre", "oracle", "ojdbc10.jar")
        if not os.path.exists(self.jar_path):
            raise FileNotFoundError(f"JDBC driver not found: {self.jar_path}")
        
        # JVM 시작 (이미 시작된 경우 무시)
        if not jpype.isJVMStarted():
            # C:\jdk25\bin\server\jvm.dll 사용
            jvm_path = r"C:\jdk25\bin\server\jvm.dll"
            if not os.path.exists(jvm_path):
                logger.warning(f"JVM path not found: {jvm_path}, using default JVM")
                jvm_path = jpype.getDefaultJVMPath()
            
            logger.info(f"Starting JVM with: {jvm_path}")
            jpype.startJVM(
                jvm_path,
                f"-Djava.class.path={self.jar_path}",
                "-Xmx512m"
            )

    def init_pool(self):
        def connector():
            jdbc_url = f"jdbc:oracle:thin:@{self.config['db_host']}:{self.config['db_port']}:{self.config['db_name']}"
            logger.info(f"Connecting to Oracle: {jdbc_url}")
            conn = jaydebeapi.connect(
                "oracle.jdbc.driver.OracleDriver",
                jdbc_url,
                [self.config['user'], self.config['password']],
                self.jar_path
            )
            # JDBC 연결 시 autocommit 비활성화 (명시적 commit 사용)
            conn.jconn.setAutoCommit(False)
            logger.debug("Oracle connection created with autocommit=False")
            return conn
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self): return self.pool.acquire()
    def release_connection(self, conn): self.pool.release(conn)

    def check_and_drop_objects(self, conn):
        """테이블과 시퀀스 존재 여부 확인 후 삭제"""
        cursor = conn.cursor()
        try:
            # 테이블 존재 확인
            cursor.execute("""
                SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'LOAD_TEST'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            # 시퀀스 존재 확인
            cursor.execute("""
                SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'LOAD_TEST_SEQ'
            """)
            seq_exists = cursor.fetchone()[0] > 0
            
            # 테이블이 있으면 삭제
            if table_exists:
                logger.info("Dropping existing LOAD_TEST table...")
                cursor.execute("DROP TABLE LOAD_TEST PURGE")
                conn.commit()  # 명시적 commit
                logger.info("LOAD_TEST table dropped successfully")
            else:
                logger.info("LOAD_TEST table does not exist, will create new")
            
            # 시퀀스가 있으면 삭제
            if seq_exists:
                logger.info("Dropping existing LOAD_TEST_SEQ sequence...")
                cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
                conn.commit()  # 명시적 commit
                logger.info("LOAD_TEST_SEQ sequence dropped successfully")
            else:
                logger.info("LOAD_TEST_SEQ sequence does not exist, will create new")
                
        except Exception as e:
            logger.error(f"Error checking/dropping objects: {e}")
            conn.rollback()  # 명시적 rollback
        finally:
            cursor.close()

    def get_ddl_statements(self):
        return [
            "CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE",
            """CREATE TABLE LOAD_TEST (
                ID NUMBER PRIMARY KEY,
                THREAD_ID VARCHAR2(50),
                VALUE_COL VARCHAR2(100),
                CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                STATUS VARCHAR2(20) DEFAULT 'ACTIVE')"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        cursor = conn.cursor()
        try:
            # INSERT
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS)
                VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, 'ACTIVE')
            """, (thread_id, f"VAL_{iteration}"))
            
            # Get the last inserted ID
            cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
            val = cursor.fetchone()[0]
            conn.commit()  # 명시적 commit
            return int(val)
        except Exception as e:
            conn.rollback()  # 에러 시 명시적 rollback
            raise e
        finally:
            cursor.close()

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
            return cursor.fetchone() is not None
        finally:
            cursor.close()

# --- Tibero (JDBC) ---
class TiberoAdapter(DatabaseAdapter):
    def __init__(self, config):
        super().__init__(config)
        if not JDBC_AVAILABLE:
            raise ImportError("jaydebeapi and jpype are required for Tibero JDBC connection")
        
        # JDBC 드라이버 경로 설정 (./jre/tibero/tibero7-jdbc.jar)
        self.jar_path = os.path.join(os.path.dirname(__file__), "jre", "tibero", "tibero7-jdbc.jar")
        if not os.path.exists(self.jar_path):
            raise FileNotFoundError(f"JDBC driver not found: {self.jar_path}")
        
        # JVM 시작 (이미 시작된 경우 무시)
        if not jpype.isJVMStarted():
            # C:\jdk25\bin\server\jvm.dll 사용
            jvm_path = r"C:\jdk25\bin\server\jvm.dll"
            if not os.path.exists(jvm_path):
                logger.warning(f"JVM path not found: {jvm_path}, using default JVM")
                jvm_path = jpype.getDefaultJVMPath()
            
            logger.info(f"Starting JVM with: {jvm_path}")
            jpype.startJVM(
                jvm_path,
                f"-Djava.class.path={self.jar_path}",
                "-Xmx512m"
            )

    def init_pool(self):
        def connector():
            jdbc_url = f"jdbc:tibero:thin:@{self.config['db_host']}:{self.config['db_port']}:{self.config['db_name']}"
            logger.info(f"Connecting to Tibero: {jdbc_url}")
            conn = jaydebeapi.connect(
                "com.tmax.tibero.jdbc.TbDriver",
                jdbc_url,
                [self.config['user'], self.config['password']],
                self.jar_path
            )
            # JDBC 연결 시 autocommit 비활성화 (명시적 commit 사용)
            conn.jconn.setAutoCommit(False)
            logger.debug("Tibero connection created with autocommit=False")
            return conn
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self): return self.pool.acquire()
    def release_connection(self, conn): self.pool.release(conn)

    def check_and_drop_objects(self, conn):
        """테이블과 시퀀스 존재 여부 확인 후 삭제"""
        cursor = conn.cursor()
        try:
            # 테이블 존재 확인
            cursor.execute("""
                SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'LOAD_TEST'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            # 시퀀스 존재 확인
            cursor.execute("""
                SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'LOAD_TEST_SEQ'
            """)
            seq_exists = cursor.fetchone()[0] > 0
            
            # 테이블이 있으면 삭제
            if table_exists:
                logger.info("Dropping existing LOAD_TEST table...")
                cursor.execute("DROP TABLE LOAD_TEST PURGE")
                conn.commit()  # 명시적 commit
                logger.info("LOAD_TEST table dropped successfully")
            else:
                logger.info("LOAD_TEST table does not exist, will create new")
            
            # 시퀀스가 있으면 삭제
            if seq_exists:
                logger.info("Dropping existing LOAD_TEST_SEQ sequence...")
                cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
                conn.commit()  # 명시적 commit
                logger.info("LOAD_TEST_SEQ sequence dropped successfully")
            else:
                logger.info("LOAD_TEST_SEQ sequence does not exist, will create new")
                
        except Exception as e:
            logger.error(f"Error checking/dropping objects: {e}")
            conn.rollback()  # 명시적 rollback
        finally:
            cursor.close()

    def get_ddl_statements(self):
        return [
            "CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE",
            """CREATE TABLE LOAD_TEST (
                ID NUMBER PRIMARY KEY,
                THREAD_ID VARCHAR2(50),
                VALUE_COL VARCHAR2(100),
                CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                STATUS VARCHAR2(20) DEFAULT 'ACTIVE')"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS)
                VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, 'ACTIVE')
            """, (thread_id, f"VAL_{iteration}"))
            
            cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
            val = cursor.fetchone()[0]
            conn.commit()  # 명시적 commit
            return int(val)
        except Exception as e:
            conn.rollback()  # 에러 시 명시적 rollback
            raise e
        finally:
            cursor.close()

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
            return cursor.fetchone() is not None
        finally:
            cursor.close()

# -------------------------------------------------------------------------
# [3] Main Controller
# -------------------------------------------------------------------------
class MultiDBLoadTester:
    def __init__(self, db_type, config):
        self.stats = {'insert': 0, 'select': 0, 'error': 0}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        if db_type == 'oracle': self.adapter = OracleAdapter(config)
        elif db_type == 'tibero': self.adapter = TiberoAdapter(config)
        else: raise ValueError(f"Unknown DB Type: {db_type}")

    def setup_database(self):
        logger.info("Initializing Database (Check and Drop/Create if exists)...")
        self.adapter.init_pool()
        conn = self.adapter.get_connection()
        try:
            # 1. 테이블/시퀀스 존재 확인 및 삭제
            self.adapter.check_and_drop_objects(conn)
            
            # 2. 새로 생성
            cursor = conn.cursor()
            for sql in self.adapter.get_ddl_statements():
                try: 
                    logger.info(f"Creating database objects...")
                    cursor.execute(sql)
                    conn.commit()  # 명시적 commit
                    logger.info(f"Database object created successfully")
                except Exception as e: 
                    logger.error(f"DDL Error: {e}")
                    conn.rollback()  # 명시적 rollback
            cursor.close()
            logger.info("Database setup completed successfully!")
        finally:
            self.adapter.release_connection(conn)

    def worker(self, thread_idx):
        t_name = f"W-{thread_idx}"
        i = 0
        while not self.stop_event.is_set():
            i += 1
            conn = None
            try:
                conn = self.adapter.get_connection()
                # 1. Insert
                rid = self.adapter.execute_transaction(conn, t_name, i)
                with self.lock: self.stats['insert'] += 1
                
                # 2. Select Verify
                if not self.adapter.verify_transaction(conn, rid):
                    raise RuntimeError(f"Verify Failed ID={rid}")
                with self.lock: self.stats['select'] += 1
                
            except Exception as e:
                with self.lock: self.stats['error'] += 1
                logger.debug(f"Error in {t_name}: {e}")
            finally:
                if conn: self.adapter.release_connection(conn)
                time.sleep(0.001)

    def monitor(self):
        last = 0
        while not self.stop_event.wait(5):
            with self.lock:
                cur, sel, err = self.stats['insert'], self.stats['select'], self.stats['error']
            tps = (cur - last) / 5.0
            last = cur
            logger.info(f"TPS: {tps:.1f} | Insert: {cur} | Select: {sel} | Errors: {err}")

    def run(self, threads, duration):
        self.setup_database()
        logger.info(f"Start Test: {threads} threads / {duration} sec")
        
        mon = threading.Thread(target=self.monitor, daemon=True)
        mon.start()
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(self.worker, i) for i in range(threads)]
            time.sleep(duration)
            self.stop_event.set()
        logger.info(f"Done. Final Stats: {self.stats}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-type', required=True, choices=['oracle', 'tibero'])
    
    # JDBC 연결용 파라미터
    parser.add_argument('--db-host', required=True, help="Database Host")
    parser.add_argument('--db-port', required=True, type=int, help="Database Port")
    parser.add_argument('--db-name', required=True, help="Database Name/SID")
    
    # 공통 파라미터
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--thread-count', type=int, default=10)
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--min-pool', type=int, default=5)
    parser.add_argument('--max-pool', type=int, default=20)
    
    args = parser.parse_args()
    cfg = {k:v for k,v in vars(args).items() if k not in ['db_type']}
    
    MultiDBLoadTester(args.db_type, cfg).run(args.thread_count, args.duration)