#!/usr/bin/env python3
"""
Universal Database Load Tester
지원 DB: Oracle, Tibero, SQL Server, PostgreSQL, MySQL
작성자: Windows Server Infrastructure Architect (MCM)

# 가상 환경 생성 (권장)
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# 필수 패키지 설치
pip install python-dotenv

# [Oracle] (기존과 동일)
pip install python-oracledb

# [PostgreSQL]
pip install psycopg2-binary

# [MySQL/MariaDB]
pip install pymysql

# [SQL Server & Tibero] (ODBC 드라이버 필요)
pip install pyodbc

SQL Server: ODBC Driver for SQL Server 설치가 필요합니다.
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17
Tibero: Tibero 클라이언트 설치 및 tbdsn.tbr 설정이 완료되어 있어야 하며, 
ODBC 데이터 원본 관리자(DSN)에 Tibero 드라이버가 등록되어 있어야 합니다.

"""

import sys
import time
import logging
import threading
import argparse
import random
import queue
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, Tuple
from dotenv import load_dotenv
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DBLoadTester")

# --- [1] Generic Connection Pool (for drivers without native pooling) ---
class GenericConnectionPool:
    """ODBC, MySQL 등 내장 풀이 취약한 드라이버를 위한 스레드 안전 큐 기반 풀"""
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
            # 풀이 비어있고 최대 크기 미만이면 생성 시도
            with self.lock:
                if self.current_size < self.max_size:
                    conn = self.connector_func()
                    self.current_size += 1
                    return conn
            # 생성 불가시 다시 대기 (혹은 에러)
            return self.pool.get(block=True, timeout=timeout)

    def release(self, conn):
        try:
            # 연결 상태 확인 (간단한 체크)
            self.pool.put(conn)
        except queue.Full:
            # 풀이 꽉 찼으면 연결 종료
            try:
                conn.close()
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

# --- [2] Database Adapter Strategy ---

class DatabaseAdapter(ABC):
    """모든 데이터베이스가 상속받아야 할 추상 기본 클래스"""
    def __init__(self, config: Dict[str, Any]):
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
        """테이블 생성 DDL 리스트 반환"""
        pass

    @abstractmethod
    def execute_transaction(self, conn, thread_id: str, iteration: int) -> int:
        """INSERT 후 생성된 ID 반환"""
        pass

    @abstractmethod
    def verify_transaction(self, conn, row_id: int) -> bool:
        """SELECT로 데이터 검증"""
        pass

# -------------------------------------------------------------------------
# [Oracle Implementation]
# -------------------------------------------------------------------------
class OracleAdapter(DatabaseAdapter):
    def __init__(self, config):
        import oracledb
        self.oracledb = oracledb
        super().__init__(config)

    def init_pool(self):
        if self.config.get('thick_mode', False):
            try:
                self.oracledb.init_oracle_client()
            except Exception as e:
                logger.warning(f"Thick 모드 초기화 실패 (이미 초기화됨?): {e}")

        self.pool = self.oracledb.create_pool(
            user=self.config['user'],
            password=self.config['password'],
            dsn=self.config['dsn'],
            min=self.config['min_pool'],
            max=self.config['max_pool'],
            increment=self.config.get('increment', 1),
            getmode=self.oracledb.POOL_GETMODE_WAIT
        )

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
               )""" # 파티셔닝은 옵션 (필요시 추가)
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        with conn.cursor() as cursor:
            out_id = cursor.var(self.oracledb.NUMBER)
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS)
                VALUES (LOAD_TEST_SEQ.NEXTVAL, :1, :2, 'ACTIVE')
                RETURNING ID INTO :3
            """, [thread_id, f"VAL_{iteration}", out_id])
            inserted_id = out_id.getvalue()[0]
            conn.commit()
            return inserted_id

    def verify_transaction(self, conn, row_id):
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = :1", [row_id])
            row = cursor.fetchone()
            return row is not None and row[0] == row_id

# -------------------------------------------------------------------------
# [PostgreSQL Implementation]
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
            dsn=self.config['dsn'], # "dbname=test user=postgres password=secret host=localhost"
            user=self.config.get('user'),
            password=self.config.get('password')
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
# [MySQL Implementation]
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
            # pymysql returns tuple or dict depending on cursor
            if row:
                val = row[0] if isinstance(row, tuple) else row['ID']
                return val == row_id
            return False

# -------------------------------------------------------------------------
# [SQL Server Implementation] (via ODBC)
# -------------------------------------------------------------------------
class SQLServerAdapter(DatabaseAdapter):
    def __init__(self, config):
        import pyodbc
        self.pyodbc = pyodbc
        super().__init__(config)

    def init_pool(self):
        def connector():
            # DSN less connection string recommended
            conn_str = f"DRIVER={{{self.config['driver']}}};SERVER={self.config['host']},{self.config.get('port', 1433)};DATABASE={self.config['database']};UID={self.config['user']};PWD={self.config['password']}"
            return self.pyodbc.connect(conn_str)
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self):
        return self.pool.acquire()

    def release_connection(self, conn):
        self.pool.release(conn)

    def get_ddl_statements(self):
        return [
            """CREATE TABLE LOAD_TEST (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                THREAD_ID VARCHAR(50),
                VALUE_COL VARCHAR(100),
                CREATED_AT DATETIME DEFAULT GETDATE(),
                STATUS VARCHAR(20) DEFAULT 'ACTIVE'
            )"""
        ]

    def execute_transaction(self, conn, thread_id, iteration):
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO LOAD_TEST (THREAD_ID, VALUE_COL, STATUS)
            VALUES (?, ?, 'ACTIVE');
            SELECT SCOPE_IDENTITY();
        """, (thread_id, f"VAL_{iteration}"))
        row = cursor.fetchone()
        inserted_id = int(row[0])
        conn.commit()
        cursor.close()
        return inserted_id

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
        row = cursor.fetchone()
        cursor.close()
        return row is not None and row[0] == row_id

# -------------------------------------------------------------------------
# [Tibero Implementation] (via ODBC)
# -------------------------------------------------------------------------
class TiberoAdapter(DatabaseAdapter):
    """Tibero는 Oracle과 SQL이 유사하지만 Python에서는 주로 ODBC로 연결함"""
    def __init__(self, config):
        import pyodbc
        self.pyodbc = pyodbc
        super().__init__(config)

    def init_pool(self):
        def connector():
            # Tibero ODBC DSN 연결
            # DSN=TiberoDSN;UID=user;PWD=pass
            conn_str = f"DSN={self.config['dsn_name']};UID={self.config['user']};PWD={self.config['password']}"
            return self.pyodbc.connect(conn_str)
        self.pool = GenericConnectionPool(connector, self.config['min_pool'], self.config['max_pool'])

    def get_connection(self):
        return self.pool.acquire()

    def release_connection(self, conn):
        self.pool.release(conn)

    def get_ddl_statements(self):
        # Tibero DDL is Oracle compatible
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
        # ODBC는 일반적으로 ? placeholder 사용
        # Tibero의 경우 SEQUENCE를 직접 호출
        cursor.execute("""
            INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, STATUS)
            VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, 'ACTIVE')
        """, (thread_id, f"VAL_{iteration}"))
        
        # INSERT 직후 ID를 가져오기 위해 Sequence CURRVAL 사용 (같은 세션)
        # 주의: 멀티스레드 환경에서 CURRVAL은 현재 세션 값을 보장함
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        inserted_id = int(cursor.fetchone()[0])
        conn.commit()
        cursor.close()
        return inserted_id

    def verify_transaction(self, conn, row_id):
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM LOAD_TEST WHERE ID = ?", (row_id,))
        row = cursor.fetchone()
        cursor.close()
        return row is not None and row[0] == row_id

# --- [3] Main Load Tester Application ---

class MultiDBLoadTester:
    def __init__(self, db_type, config):
        self.stats = {'insert': 0, 'select': 0, 'error': 0}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # 어댑터 팩토리
        if db_type == 'oracle':
            self.adapter = OracleAdapter(config)
        elif db_type == 'mysql':
            self.adapter = MySQLAdapter(config)
        elif db_type == 'postgres':
            self.adapter = PostgresAdapter(config)
        elif db_type == 'sqlserver':
            self.adapter = SQLServerAdapter(config)
        elif db_type == 'tibero':
            self.adapter = TiberoAdapter(config)
        else:
            raise ValueError(f"지원하지 않는 DB 타입: {db_type}")

    def setup_database(self):
        """테이블 초기화"""
        logger.info("데이터베이스 연결 및 초기화 중...")
        try:
            # 1회성 연결로 DDL 수행
            self.adapter.init_pool()
            conn = self.adapter.get_connection()
            
            # 기존 객체 삭제 시도 (에러 무시)
            cursor = conn.cursor()
            drop_sqls = [
                "DROP TABLE LOAD_TEST", 
                "DROP SEQUENCE LOAD_TEST_SEQ"
            ]
            for sql in drop_sqls:
                try:
                    cursor.execute(sql)
                    conn.commit()
                except Exception:
                    conn.rollback()
            
            # 생성
            for sql in self.adapter.get_ddl_statements():
                try:
                    cursor.execute(sql)
                    conn.commit()
                    logger.info(f"DDL 실행 성공: {sql[:30]}...")
                except Exception as e:
                    logger.error(f"DDL 실행 실패: {e}")
                    raise
            
            cursor.close()
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
                    try:
                        self.adapter.release_connection(conn)
                    except:
                        pass
                # CPU 과점 방지
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
    parser = argparse.ArgumentParser(description="Multi-DB Load Tester")
    parser.add_argument('--db-type', required=True, choices=['oracle', 'mysql', 'postgres', 'sqlserver', 'tibero'])
    parser.add_argument('--host', help="DB Host")
    parser.add_argument('--port', type=int, help="DB Port")
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--database', help="Database Name / Service Name / SID")
    parser.add_argument('--dsn-name', help="ODBC DSN Name (for Tibero/SQLServer)")
    parser.add_argument('--driver', default='ODBC Driver 17 for SQL Server', help="ODBC Driver Name")
    parser.add_argument('--thread-count', type=int, default=10)
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--min-pool', type=int, default=10)
    parser.add_argument('--max-pool', type=int, default=20)
    
    args = parser.parse_args()
    
    # Config Dictionary 구성
    config = {
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'dsn': args.database, # PostgreSQL/Oracle uses this as connect string often
        'dsn_name': args.dsn_name, # ODBC uses this
        'driver': args.driver,
        'min_pool': args.min_pool,
        'max_pool': args.max_pool
    }
    
    tester = MultiDBLoadTester(args.db_type, config)
    tester.run(args.thread_count, args.duration)

if __name__ == "__main__":
    main()
