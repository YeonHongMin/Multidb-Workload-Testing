"""
# Requirements.txt
SQLAlchemy>=2.0.0
python-oracledb>=2.0.0
PyMySQL>=1.0.0
psycopg2-binary>=2.9.0
pyodbc>=4.0.0

# Code Start
Cross-database load tester that works with Oracle, Tibero, PostgreSQL, MySQL, and SQL Server.

The script uses SQLAlchemy connection pooling and emits dialect-agnostic DDL for a simple table
that is exercised via INSERT -> COMMIT -> SELECT loops across multiple worker threads.

Example (SQLAlchemy URL style):

python oracle_load_tester_codex.py \
  --db-type oracle \
  --db-url "oracle+oracledb://user:password@host:1521/?service_name=orclpdb1" \
  --apply-ddl \
  --thread-count 50 \
  --test-duration-seconds 120

If --db-url is omitted, the script attempts to build one from the host/user/password/name
parameters plus an optional DBAPI driver.

Driver expectations by database:
* Oracle/Tibero: python-oracledb (treated with the Oracle dialect)
* PostgreSQL: psycopg2
* MySQL: PyMySQL
* SQL Server: pyodbc with "ODBC Driver 17 for SQL Server" by default
"""
from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from dataclasses import dataclass, field

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-database load tester using SQLAlchemy")
    parser.add_argument("--db-type", choices=["oracle", "tibero", "postgres", "mysql", "sqlserver"], required=True)
    parser.add_argument("--db-url", default=os.environ.get("DB_URL"), help="SQLAlchemy URL. If omitted, built from other flags")
    parser.add_argument("--db-user", default=os.environ.get("DB_USER"), help="Database username")
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD"), help="Database password")
    parser.add_argument("--db-host", default=os.environ.get("DB_HOST", "localhost"), help="Database host when constructing URL")
    parser.add_argument("--db-port", default=os.environ.get("DB_PORT"), help="Database port when constructing URL")
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME"), help="Database/service name when constructing URL")
    parser.add_argument("--db-driver", default=os.environ.get("DB_DRIVER"), help="Optional DBAPI driver (e.g., pymysql, psycopg2, oracledb, pyodbc)")
    parser.add_argument("--thread-count", type=int, default=int(os.environ.get("THREAD_COUNT", 50)), help="Number of worker threads")
    parser.add_argument("--test-duration-seconds", type=int, default=int(os.environ.get("TEST_DURATION_SECONDS", 120)), help="How long to run the test")
    parser.add_argument("--min-pool-size", type=int, default=int(os.environ.get("MIN_POOL_SIZE", 10)), help="Connection pool size (pool_size)")
    parser.add_argument("--max-pool-size", type=int, default=int(os.environ.get("MAX_POOL_SIZE", 50)), help="Maximum pool size (pool_size + max_overflow)")
    parser.add_argument("--monitor-interval", type=int, default=int(os.environ.get("MONITOR_INTERVAL", 5)), help="Seconds between metrics logs")
    parser.add_argument("--print-ddl", action="store_true", help="Print the generated DDL and exit")
    parser.add_argument("--apply-ddl", action="store_true", help="Create/drop tables before running")
    parser.add_argument("--ddl-drop-existing", action="store_true", help="Drop existing table before creation")
    parser.add_argument("--table-name", default=os.environ.get("TABLE_NAME", "load_test"), help="Table name for the load test")
    return parser.parse_args()


def build_url(args: argparse.Namespace) -> str:
    if args.db_url:
        return args.db_url

    if not args.db_user or not args.db_password or not args.db_name:
        raise ValueError("db-url is missing and db-user/db-password/db-name are required to build one")

    driver = f"+{args.db_driver}" if args.db_driver else ""
    host = args.db_host
    port_fragment = f":{args.db_port}" if args.db_port else ""
    name = args.db_name

    if args.db_type in {"oracle", "tibero"}:
        return f"oracle{driver}://{args.db_user}:{args.db_password}@{host}{port_fragment}/?service_name={name}"
    if args.db_type == "postgres":
        return f"postgresql{driver}://{args.db_user}:{args.db_password}@{host}{port_fragment}/{name}"
    if args.db_type == "mysql":
        return f"mysql{driver}://{args.db_user}:{args.db_password}@{host}{port_fragment}/{name}"
    if args.db_type == "sqlserver":
        if args.db_driver:
            driver_qs = f"driver={args.db_driver}"
        else:
            driver_qs = "driver=ODBC Driver 17 for SQL Server"
        return f"mssql+pyodbc://{args.db_user}:{args.db_password}@{host}{port_fragment}/{name}?{driver_qs}"

    raise ValueError(f"Unsupported db-type: {args.db_type}")


def make_metadata(table_name: str) -> MetaData:
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("thread_id", String(64), nullable=False),
        Column("value_col", String(1024), nullable=False),
        Column("filler1", String(1024)),
        Column("filler2", String(1024)),
        Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    )
    return metadata


def create_engine_pool(args: argparse.Namespace) -> Engine:
    if args.min_pool_size < 1 or args.max_pool_size < args.min_pool_size:
        raise ValueError("Pool size must be positive and max_pool_size must be >= min_pool_size")

    url = build_url(args)
    max_overflow = max(args.max_pool_size - args.min_pool_size, 0)
    logging.info("Connecting to %s with pool_size=%d max_overflow=%d", args.db_type, args.min_pool_size, max_overflow)
    engine = create_engine(
        url,
        pool_size=args.min_pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        future=True,
    )
    return engine


def format_ddl(metadata: MetaData) -> str:
    ddl_lines: list[str] = []
    for table in metadata.sorted_tables:
        ddl_lines.append(str(table.to_metadata(MetaData()).compile(compile_kwargs={"literal_binds": True})))
    return "\n\n".join(ddl_lines)


def apply_ddl(args: argparse.Namespace, engine: Engine, metadata: MetaData) -> None:
    with engine.begin() as conn:
        if args.ddl_drop_existing:
            metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn, checkfirst=True)
    logging.info("DDL applied successfully")


def worker_loop(
    engine: Engine,
    metrics: Metrics,
    stop_event: threading.Event,
    thread_label: str,
    table,
) -> None:
    filler_payload = "X" * 256
    insert_stmt = table.insert().returning(table.c.id)

    while not stop_event.is_set():
        try:
            with engine.begin() as conn:
                inserted_value = f"payload-{thread_label}-{time.time()}"
                result = conn.execute(
                    insert_stmt,
                    {
                        "thread_id": thread_label,
                        "value_col": inserted_value,
                        "filler1": filler_payload,
                        "filler2": filler_payload,
                    },
                )
                inserted_id = result.scalar_one()

                row = conn.execute(select(table.c.value_col).where(table.c.id == inserted_id)).fetchone()
                if not row:
                    raise RuntimeError(f"Row not found for id {inserted_id}")

            metrics.record_success()
        except SQLAlchemyError as exc:
            metrics.record_error()
            logging.exception("Thread %s database error: %s", thread_label, exc)
        except Exception as exc:  # broad to keep threads alive
            metrics.record_error()
            logging.exception("Thread %s error: %s", thread_label, exc)


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


def run_workers(args: argparse.Namespace, engine: Engine, table) -> None:
    metrics = Metrics()
    stop_event = threading.Event()

    threads = [
        threading.Thread(
            target=worker_loop,
            name=f"worker-{i}",
            args=(engine, metrics, stop_event, f"worker-{i}", table),
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


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    )

    metadata = make_metadata(args.table_name)
    engine = create_engine_pool(args)
    table = metadata.tables[args.table_name]

    if args.print_ddl:
        print(format_ddl(metadata))
        return

    if args.apply_ddl:
        apply_ddl(args, engine, metadata)

    try:
        run_workers(args, engine, table)
    finally:
        engine.dispose()
        logging.info("Connection pool disposed")


if __name__ == "__main__":
    main()
