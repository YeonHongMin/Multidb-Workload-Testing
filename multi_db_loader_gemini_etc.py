#!/usr/bin/env python3
"""
[Universal Database Load Tester]
- 지원 DB: Oracle, Tibero, SQL Server, PostgreSQL, MySQL
- 기능: 멀티스레드 부하 테스트 (INSERT -> COMMIT -> SELECT 검증)
- 작성: Windows Server Infrastructure Architect
- 주의: 실행 시 'LOAD_TEST' 테이블이 초기화(DROP/CREATE) 됩니다.

1. Oracle (oracledb)
python universal_load_tester.py --db-type oracle --user TEST_USER --password password123 --database "localhost:1521/ORCL" --thread-count 50

2. MySQL(pymysql)
python universal_load_tester.py --db-type mysql --host localhost --user root --password password123 --database test_db --thread-count 50

3. PostgreSQL (psycopg2)
python universal_load_tester.py --db-type postgres --database "host=localhost dbname=testdb user=postgres password=secret" --user postgres --password secret --thread-count 50

4. Tibero (ODBC - pyodbc) 전제: Windows ODBC 데이터 원본 관리자 또는 Linux odbc.ini에 TiberoDSN이라는 이름으로 DSN이 등록되어 있어야 합니다.
python universal_load_tester.py --db-type tibero --dsn-name TiberoDSN --user tibero --password tmax --thread-count 50

5. SQL Server (ODBC - pyodbc)
python universal_load_tester.py --db-type sqlserver --host 192.168.1.100 --database Master --user sa --password password123 --thread-count 50


"""

import sys
import time
import logging
import threading
import argparse
import queue
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DBLoadTester")

# -------------------------------------------------------------------------
# [1] Generic Connection Pool (ODBC/MySQL용 - 내장 풀 없는 경우)
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

# --- Oracle ---
class OracleAdapter(DatabaseAdapter):
    def __init__(self, config):
        import oracledb
        self.oracledb = oracledb
        super().__init__(config)

    def init_pool(self):
        if self.config.get('thick_mode', False):
            try: self.oracledb.init_oracle_client()
            except: pass
        
        self.pool = self.oracledb.create_pool(
            user=self.config['user'], password=self.config['password'],
            dsn=self.config['dsn'], min=self.config['min_pool'],
            max=self.config['max_pool'], increment=1,
            getmode=self.oracledb.POOL_GETMODE_WAIT
        )

    def get_connection(self): return self.pool.acquire()
    def release_connection(self, conn): self.pool.release(conn)

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
        with conn.cursor() as cursor:
            out_id = cursor.var(self.oracledb.NUMBER)
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS)
                VALUES (LOAD_TEST_SEQ.NEXTVAL, :1, :2, 'ACTIVE')
                RETURNING ID INTO :3
            """, [thread_id, f"VAL_{iteration}", out_id])
            val = out_id.getvalue()
            conn.commit()
            return val[0]

    def verify_transaction(self, conn, row_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = :1", [row_id])
            return cursor.fetchone() is not None

# --- PostgreSQL ---
class PostgresAdapter(DatabaseAdapter):
    def __init__(self, config):
        import psycopg2
        from psycopg2 import pool
        self.psycopg2 = psycopg2
        self.pg_pool_cls = pool.ThreadedConnectionPool
        super().__init__(config)

    def init_pool(self):
        self.pool = self.pg_pool_cls(
            minconn=self.config['min_pool'], maxconn=self.config['max_pool'],
            dsn=self.config['dsn'], user=self.config.get('user'),
            password=self.config.get('password')
        )

    def get_connection(self): return self.pool.getconn()
    def release_connection(self, conn): self.pool.putconn(conn)

    def get_ddl_statements(self):
        return ["""
            CREATE TABLE LOAD_TEST (
                ID SERIAL PRIMARY KEY,
                THREAD_ID VARCHAR(50),
                VALUE_COL VARCHAR(100),
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                STATUS VARCHAR(20) DEFAULT 'ACTIVE')
        """]

    def execute_transaction(self, conn, thread_id, iteration):
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO LOAD_TEST (THREAD_ID, VALUE_COL, STATUS)
                VALUES (%s, %s, 'ACTIVE') RETURNING ID
            """, (thread_id, f"VAL_{iteration}"))
            val = cursor.fetchone()[0]
            conn.commit()
            return val

    def verify_transaction(self, conn, row_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = %s", (row_id,))
            return cursor.fetchone() is not None

# --- MySQL ---
class MySQLAdapter(DatabaseAdapter):
    def __init__(self, config):
        import pymysql
        self.pymysql = pymysql
        super().__init__(config)

    def init_pool(self):
        def connector():
            return self.pymysql.connect(
                host=self.config['host'], user=self.config['user'],
                password=self.config['password'], database=self.config['database'],
                port=int(self.config.get('port', 3306)), autocommit=False
            )
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self): return self.pool.acquire()
    def release_connection(self, conn): self.pool.release(conn)

    def get_ddl_statements(self):
        return ["""
            CREATE TABLE LOAD_TEST (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                THREAD_ID VARCHAR(50),
                VALUE_COL VARCHAR(100),
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                STATUS VARCHAR(20) DEFAULT 'ACTIVE') ENGINE=InnoDB
        """]

    def execute_transaction(self, conn, thread_id, iteration):
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO LOAD_TEST (THREAD_ID, VALUE_COL, STATUS) VALUES (%s, %s, 'ACTIVE')", 
                         (thread_id, f"VAL_{iteration}"))
            val = cursor.lastrowid
            conn.commit()
            return val

    def verify_transaction(self, conn, row_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = %s", (row_id,))
            return cursor.fetchone() is not None

# --- SQL Server (ODBC) ---
class SQLServerAdapter(DatabaseAdapter):
    def __init__(self, config):
        import pyodbc
        self.pyodbc = pyodbc
        super().__init__(config)

    def init_pool(self):
        def connector():
            conn_str = (f"DRIVER={{{self.config['driver']}}};SERVER={self.config['host']},{self.config.get('port', 1433)};"
                        f"DATABASE={self.config['database']};UID={self.config['user']};PWD={self.config['password']}")
            return self.pyodbc.connect(conn_str)
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self): return self.pool.acquire()
    def release_connection(self, conn): self.pool.release(conn)

    def get_ddl_statements(self):
        return ["""
            CREATE TABLE LOAD_TEST (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                THREAD_ID VARCHAR(50),
                VALUE_COL VARCHAR(100),
                CREATED_AT DATETIME DEFAULT GETDATE(),
                STATUS VARCHAR(20) DEFAULT 'ACTIVE')
        """]

    def execute_transaction(self, conn, thread_id, iteration):
        cursor = conn.cursor()
        cursor.execute("INSERT INTO LOAD_TEST (THREAD_ID, VALUE_COL, STATUS) VALUES (?, ?, 'ACTIVE'); SELECT SCOPE_IDENTITY();", 
                     (thread_id, f"VAL_{iteration}"))
        val = int(cursor.fetchone()[0])
        conn.commit()
        cursor.close()
        return val

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
        res = cursor.fetchone()
        cursor.close()
        return res is not None

# --- Tibero (ODBC) ---
class TiberoAdapter(DatabaseAdapter):
    def __init__(self, config):
        import pyodbc
        self.pyodbc = pyodbc
        super().__init__(config)

    def init_pool(self):
        def connector():
            # DSN=TiberoDSN;UID=user;PWD=pass
            return self.pyodbc.connect(f"DSN={self.config['dsn_name']};UID={self.config['user']};PWD={self.config['password']}")
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self): return self.pool.acquire()
    def release_connection(self, conn): self.pool.release(conn)

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
        cursor.execute("INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS) VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, 'ACTIVE')", 
                     (thread_id, f"VAL_{iteration}"))
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        val = int(cursor.fetchone()[0])
        conn.commit()
        cursor.close()
        return val

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
        res = cursor.fetchone()
        cursor.close()
        return res is not None

# -------------------------------------------------------------------------
# [3] Main Controller
# -------------------------------------------------------------------------
class MultiDBLoadTester:
    def __init__(self, db_type, config):
        self.stats = {'insert': 0, 'select': 0, 'error': 0}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        if db_type == 'oracle': self.adapter = OracleAdapter(config)
        elif db_type == 'mysql': self.adapter = MySQLAdapter(config)
        elif db_type == 'postgres': self.adapter = PostgresAdapter(config)
        elif db_type == 'sqlserver': self.adapter = SQLServerAdapter(config)
        elif db_type == 'tibero': self.adapter = TiberoAdapter(config)
        else: raise ValueError(f"Unknown DB Type: {db_type}")

    def setup_database(self):
        logger.info("Initializing Database (Drop/Create)...")
        self.adapter.init_pool()
        conn = self.adapter.get_connection()
        try:
            cursor = conn.cursor()
            for sql in ["DROP TABLE LOAD_TEST", "DROP SEQUENCE LOAD_TEST_SEQ"]:
                try: 
                    cursor.execute(sql)
                    conn.commit()
                except: conn.rollback()
            
            for sql in self.adapter.get_ddl_statements():
                try: 
                    cursor.execute(sql)
                    conn.commit()
                except Exception as e: logger.error(f"DDL Error: {e}")
            cursor.close()
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
    parser.add_argument('--db-type', required=True, choices=['oracle', 'mysql', 'postgres', 'sqlserver', 'tibero'])
    parser.add_argument('--host')
    parser.add_argument('--port', type=int)
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--database', help="DB Name / SID")
    parser.add_argument('--dsn-name', help="ODBC DSN Name")
    parser.add_argument('--driver', default='ODBC Driver 17 for SQL Server')
    parser.add_argument('--thread-count', type=int, default=10)
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--min-pool', type=int, default=10)
    parser.add_argument('--max-pool', type=int, default=20)
    
    args = parser.parse_args()
    cfg = {k:v for k,v in vars(args).items() if k != 'db_type'}
    cfg['dsn'] = args.database # Alias for some drivers
    
    MultiDBLoadTester(args.db_type, cfg).run(args.thread_count, args.duration)