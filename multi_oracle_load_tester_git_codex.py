"""
Cross-database load tester that works with Oracle, Tibero, PostgreSQL, MySQL, and SQL Server using JDBC drivers.

Requirements:
- JPype1
- JayDeBeApi
- Java (C:\\jdk25)
- JDBC drivers in ./jre/{db_type}/

The script uses a custom JDBC connection pool and executes raw SQL.
"""
from __future__ import annotations

import argparse
import logging
import os
import threading
import time
import glob
import sys
import queue
from dataclasses import dataclass, field
from typing import Optional, List

import jaydebeapi
import jpype

# Set JAVA_HOME explicitly
JAVA_HOME = r'C:\jdk25'
os.environ['JAVA_HOME'] = JAVA_HOME

@dataclass
class Metrics:
    """Thread-safe counters for throughput and errors."""

    total_ops: int = 0
    errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record_success(self, count: int = 1) -> None:
        with self.lock:
            self.total_ops += count

    def record_error(self, count: int = 1) -> None:
        with self.lock:
            self.errors += count

    def snapshot(self) -> tuple[int, int]:
        with self.lock:
            return self.total_ops, self.errors

@dataclass
class DriverInfo:
    driver_class: str
    jar_pattern: str
    url_template: str

DRIVER_MAP = {
    "oracle": DriverInfo(
        "oracle.jdbc.OracleDriver",
        "ojdbc*.jar",
        "jdbc:oracle:thin:@{host}:{port}/{service_name}"
    ),
    "tibero": DriverInfo(
        "com.tmax.tibero.jdbc.TbDriver",
        "tibero*.jar",
        "jdbc:tibero:thin:@{host}:{port}:{service_name}"
    ),
    "postgres": DriverInfo(
        "org.postgresql.Driver",
        "postgresql*.jar",
        "jdbc:postgresql://{host}:{port}/{service_name}"
    ),
    "mysql": DriverInfo(
        "com.mysql.cj.jdbc.Driver",
        "mysql*.jar",
        "jdbc:mysql://{host}:{port}/{service_name}"
    ),
    "sqlserver": DriverInfo(
        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "mssql*.jar",
        "jdbc:sqlserver://{host}:{port};databaseName={service_name}"
    ),
}

def init_jvm(args: argparse.Namespace, driver_jar_path: str):
    """Explicitly initialize the JVM with the correct classpath and DLL."""
    if jpype.isJVMStarted():
        return

    # Try to find jvm.dll in C:\jdk25
    jvm_path = None
    possible_paths = [
        os.path.join(JAVA_HOME, 'bin', 'server', 'jvm.dll'),
        os.path.join(JAVA_HOME, 'jre', 'bin', 'server', 'jvm.dll'),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            jvm_path = path
            break
            
    if not jvm_path:
        # Fallback to default lookup if specific path fails
        try:
            jvm_path = jpype.getDefaultJVMPath()
        except:
            logging.warning(f"Could not find jvm.dll in {JAVA_HOME}, letting JPype decide.")

    logging.info(f"Initializing JVM using: {jvm_path}")
    logging.info(f"Classpath: {driver_jar_path}")
    
    try:
        jpype.startJVM(jvm_path, f"-Djava.class.path={driver_jar_path}")
    except Exception as e:
        logging.error(f"Failed to start JVM: {e}")
        raise

class JDBCConnectionPool:
    def __init__(self, args: argparse.Namespace, driver_info: DriverInfo, jar_path: str):
        self.args = args
        self.driver_info = driver_info
        self.jar_path = jar_path
        self.pool = queue.Queue(maxsize=args.max_pool_size)
        self.current_size = 0
        self.lock = threading.Lock()
        
        # Construct URL
        # Note: args.db_name is mapped to service_name/database in templates
        self.url = driver_info.url_template.format(
            host=args.db_host,
            port=args.db_port,
            service_name=args.db_name
        )
        
        logging.info(f"Initializing pool for {self.url}")
        
        # Initialize JVM before creating any connections
        init_jvm(args, jar_path)
        
        # Pre-fill min pool
        for _ in range(args.min_pool_size):
            self._add_connection()

    def _create_connection(self):
        # JVM should be started by now
        conn = jaydebeapi.connect(
            self.driver_info.driver_class,
            self.url,
            [self.args.db_user, self.args.db_password],
            self.jar_path,
        )
        # Disable auto-commit to allow explicit transaction control
        try:
            conn.jconn.setAutoCommit(False)
        except Exception as e:
            logging.warning(f"Failed to set AutoCommit to False: {e}")
        return conn

    def _add_connection(self):
        with self.lock:
            if self.current_size < self.args.max_pool_size:
                try:
                    conn = self._create_connection()
                    self.pool.put(conn)
                    self.current_size += 1
                except Exception as e:
                    logging.error(f"Failed to create connection: {e}")
                    raise

    def acquire(self):
        try:
            return self.pool.get(block=True, timeout=5)
        except queue.Empty:
            # Try to grow
            with self.lock:
                if self.current_size < self.args.max_pool_size:
                    conn = self._create_connection()
                    self.current_size += 1
                    return conn
            # If we couldn't grow, wait longer or fail
            return self.pool.get(block=True, timeout=30)

    def release(self, conn):
        try:
            if not conn:
                return
            # Check if valid? (Hard with JDBC wrapper)
            self.pool.put(conn)
        except Exception:
            # If pool is full or error, close it
            try:
                conn.close()
            except:
                pass
            with self.lock:
                self.current_size -= 1

    def discard(self, conn):
        """Discard a connection (e.g. on error) and decrement pool size."""
        try:
            conn.close()
        except:
            pass
        with self.lock:
            self.current_size -= 1

    def close_all(self):
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except:
                pass

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-database load tester using JDBC")
    parser.add_argument("--db-type", choices=["oracle", "tibero", "postgres", "mysql", "sqlserver"], required=True)
    parser.add_argument("--db-user", default=os.environ.get("DB_USER"), required=True, help="Database username")
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD"), required=True, help="Database password")
    parser.add_argument("--db-host", default=os.environ.get("DB_HOST", "localhost"), help="Database host")
    parser.add_argument("--db-port", default=os.environ.get("DB_PORT"), help="Database port")
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME"), required=True, help="Database/service name")
    parser.add_argument("--thread-count", type=int, default=int(os.environ.get("THREAD_COUNT", 50)), help="Number of worker threads")
    parser.add_argument("--test-duration-seconds", type=int, default=int(os.environ.get("TEST_DURATION_SECONDS", 120)), help="How long to run the test")
    parser.add_argument("--min-pool-size", type=int, default=int(os.environ.get("MIN_POOL_SIZE", 10)), help="Connection pool size")
    parser.add_argument("--max-pool-size", type=int, default=int(os.environ.get("MAX_POOL_SIZE", 50)), help="Maximum pool size")
    parser.add_argument("--monitor-interval", type=int, default=int(os.environ.get("MONITOR_INTERVAL", 5)), help="Seconds between metrics logs")
    parser.add_argument("--print-ddl", action="store_true", help="Print the generated DDL and exit")
    parser.add_argument("--apply-ddl", action="store_true", help="Create/drop tables before running")
    parser.add_argument("--ddl-drop-existing", action="store_true", help="Drop existing table before creation")
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME", "load_test"), help="Table name for the load test")
    parser.add_argument("--jre-dir", default="./jre", help="Base directory for JDBC drivers")
    return parser.parse_args()

def find_jar(args: argparse.Namespace, driver_info: DriverInfo) -> str:
    # Look in ./jre/{db_type}/
    search_path = os.path.join(args.jre_dir, args.db_type, driver_info.jar_pattern)
    
    files = glob.glob(search_path)
    if not files:
        raise FileNotFoundError(f"Could not find JDBC jar matching {driver_info.jar_pattern} in {args.jre_dir}/{args.db_type}")
    
    # Return the last one (usually highest version if named correctly)
    return sorted(files)[-1]

def apply_ddl(args: argparse.Namespace, pool: JDBCConnectionPool):
    conn = pool.acquire()
    try:
        curs = conn.cursor()
        db_type = args.db_type
        table_name = args.table_name
        
        logging.info(f"Applying DDL for {db_type}...")
        
        if db_type in ["oracle", "tibero"]:
            # Drop Table if exists
            try:
                curs.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                logging.info(f"Dropped table {table_name}")
            except Exception as e:
                logging.warning(f"Ignored error dropping table {table_name}: {e}")

            # Drop Sequence if exists
            try:
                curs.execute(f"DROP SEQUENCE {table_name}_seq")
                logging.info(f"Dropped sequence {table_name}_seq")
            except Exception as e:
                logging.warning(f"Ignored error dropping sequence {table_name}_seq: {e}")

            # Create Sequence
            try:
                curs.execute(f"CREATE SEQUENCE {table_name}_seq START WITH 1 INCREMENT BY 1 CACHE 100")
                logging.info(f"Created sequence {table_name}_seq")
            except Exception as e:
                logging.error(f"Failed to create sequence: {e}")
                raise

            # Create Table
            try:
                curs.execute(f"CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, thread_id VARCHAR2(64), value_col VARCHAR2(1024), created_at TIMESTAMP DEFAULT SYSTIMESTAMP)")
                logging.info(f"Created table {table_name}")
            except Exception as e:
                logging.error(f"Failed to create table: {e}")
                raise

        elif db_type == "postgres":
            curs.execute(f"DROP TABLE IF EXISTS {table_name}")
            curs.execute(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    thread_id VARCHAR(64),
                    value_col VARCHAR(1024),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logging.info(f"Recreated table {table_name}")

        elif db_type == "mysql":
            curs.execute(f"DROP TABLE IF EXISTS {table_name}")
            curs.execute(f"""
                CREATE TABLE {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    thread_id VARCHAR(64),
                    value_col VARCHAR(1024),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logging.info(f"Recreated table {table_name}")

        elif db_type == "sqlserver":
            curs.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
            curs.execute(f"""
                CREATE TABLE {table_name} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    thread_id VARCHAR(64),
                    value_col VARCHAR(1024),
                    created_at DATETIME DEFAULT GETDATE()
                )
            """)
            logging.info(f"Recreated table {table_name}")
        
        curs.close()
    except Exception as e:
        logging.error(f"Failed to apply DDL: {e}")
        raise
    finally:
        pool.release(conn)

def worker_loop(
    pool: JDBCConnectionPool,
    metrics: Metrics,
    stop_event: threading.Event,
    thread_label: str,
    args: argparse.Namespace
) -> None:
    
    while not stop_event.is_set():
        conn = None
        try:
            conn = pool.acquire()
            curs = conn.cursor()
            
            inserted_value = f"payload-{thread_label}-{time.time()}"
            
            if args.db_type in ["oracle", "tibero"]:
                curs.execute(f"INSERT INTO {args.table_name} (id, thread_id, value_col) VALUES ({args.table_name}_seq.NEXTVAL, ?, ?)", (thread_label, inserted_value))
            else:
                curs.execute(f"INSERT INTO {args.table_name} (thread_id, value_col) VALUES (?, ?)", (thread_label, inserted_value))
            
            # Commit
            # JayDeBeApi usually requires explicit commit
            conn.commit()
            
            # Select back (verification)
            curs.execute(f"SELECT count(*) FROM {args.table_name} WHERE thread_id = ? AND value_col = ?", (thread_label, inserted_value))
            row = curs.fetchone()
            if not row or row[0] == 0:
                 raise RuntimeError("Row not found after insert")

            curs.close()
            metrics.record_success()
            
        except Exception as exc:
            metrics.record_error()
            logging.error(f"Thread {thread_label} error: {exc}")
            if conn:
                pool.discard(conn)
                conn = None
        finally:
            if conn:
                pool.release(conn)

def monitor_loop(metrics: Metrics, stop_event: threading.Event, interval: int) -> None:
    last_total = 0
    start_time = time.time()
    while not stop_event.wait(interval):
        total, errors = metrics.snapshot()
        delta = total - last_total
        last_total = total
        elapsed = time.time() - start_time
        tps = delta / interval if interval else 0
        logging.info(
            "Metrics: total_ops=%d errors=%d recent_tps=%.2f elapsed=%.1fs",
            total,
            errors,
            tps,
            elapsed,
        )

def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    )

    if args.db_type not in DRIVER_MAP:
        logging.error(f"Unsupported db-type: {args.db_type}")
        return

    driver_info = DRIVER_MAP[args.db_type]
    
    try:
        jar_path = find_jar(args, driver_info)
        logging.info(f"Using JDBC driver: {jar_path}")
    except FileNotFoundError as e:
        logging.error(str(e))
        return

    # Initialize Pool
    try:
        pool = JDBCConnectionPool(args, driver_info, jar_path)
    except Exception as e:
        logging.error(f"Failed to initialize connection pool: {e}")
        return

    if args.apply_ddl:
        apply_ddl(args, pool)

    metrics = Metrics()
    stop_event = threading.Event()

    threads = [
        threading.Thread(
            target=worker_loop,
            name=f"worker-{i}",
            args=(pool, metrics, stop_event, f"worker-{i}", args),
            daemon=True,
        )
        for i in range(args.thread_count)
    ]

    monitor = threading.Thread(
        target=monitor_loop,
        name="monitor",
        args=(metrics, stop_event, args.monitor_interval),
        daemon=True,
    )

    for t in threads:
        t.start()
    monitor.start()

    logging.info(
        "Started %d worker threads for %d seconds (pool min=%d, max=%d)",
        args.thread_count,
        args.test_duration_seconds,
        args.min_pool_size,
        args.max_pool_size,
    )

    time.sleep(args.test_duration_seconds)
    stop_event.set()

    for t in threads:
        t.join()
    monitor.join()

    total, errors = metrics.snapshot()
    logging.info("Test complete. total_ops=%d errors=%d", total, errors)
    
    pool.close_all()

if __name__ == "__main__":
    main()
