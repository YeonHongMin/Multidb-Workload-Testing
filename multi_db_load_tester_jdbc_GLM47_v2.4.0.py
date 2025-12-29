#!/usr/bin/env python3
"""
멀티 데이터베이스 부하 테스트 프로그램 (JDBC 드라이버 사용) - Enhanced Version v2.3
Oracle, PostgreSQL, MySQL, SingleStore, SQL Server, Tibero, DB2 지원

특징:
- ./jre 디렉터리의 JDBC 드라이버 사용
- JayDeBeApi를 통한 JDBC 연결
- 멀티스레드 + 커넥션 풀링
- INSERT -> COMMIT -> SELECT 검증 패턴
- 자동 에러 복구 및 커넥션 재연결
- 실시간 성능 모니터링 (TPS, 에러 카운트)
- **1초 이내 트랜잭션량 측정 (Sub-second Metrics)**
- **작업 모드 선택 (insert-only, select-only, update-only, delete-only, mixed, full)**
- **결과 내보내기 (CSV/JSON)**
- **워밍업 기간 설정**
- **Graceful Shutdown (Ctrl+C)**
- **배치 INSERT 지원**
- **목표 TPS 제한 (Rate Limiting)**
- **점진적 부하 증가 (Ramp-up)**
- **커넥션 풀 상태 모니터링**

사용 예시:
  # Full 모드 (INSERT -> COMMIT -> SELECT)
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --mode full

  # Insert-only 모드 (배치 크기 지정)
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --mode insert-only --batch-size 100

  # 워밍업, Ramp-up, TPS 제한 적용
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --warmup 30 --ramp-up 60 --target-tps 1000

  # 결과 CSV/JSON 내보내기
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --output-format csv --output-file results.csv
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
import signal
import json
import csv
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Union

from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
import queue

# 버전 정보 (로그 및 CLI 배너에 사용)
VERSION = "2.4"

# JDBC 드라이버 사용을 위한 라이브러리
try:
    import jaydebeapi
    import jpype
    JAYDEBEAPI_AVAILABLE = True
except ImportError:
    JAYDEBEAPI_AVAILABLE = False
    print("ERROR: jaydebeapi or jpype1 not installed. Install with: pip install jaydebeapi JPype1")
    sys.exit(1)

# 로깅 설정
log_format = '%(asctime)s - %(message)s'
log_formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')

console_format = '%(asctime)s - %(message)s'
console_formatter = logging.Formatter(console_format, datefmt='%H:%M:%S')


class BelowWarningFilter(logging.Filter):
    """WARNING 레벨 미만의 로그만 통과시키는 필터

    INFO, DEBUG 레벨 로그를 콘솔에 출력하고,
    WARNING 이상은 별도 에러 로그 파일로 분리하기 위해 사용
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """로그 레코드 필터링

        Args:
            record: 로그 레코드 객체

        Returns:
            WARNING 레벨 미만이면 True, 그렇지 않으면 False
        """
        return record.levelno < logging.WARNING

file_handler = logging.FileHandler('multi_db_load_test_jdbc.log')
file_handler.setFormatter(log_formatter)
file_handler.addFilter(BelowWarningFilter())

error_handler = logging.FileHandler('multi_db_load_test_jdbc_error.log')
error_handler.setLevel(logging.WARNING)
error_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(console_formatter)
console_handler.addFilter(BelowWarningFilter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, error_handler, console_handler]
)
logger = logging.getLogger(__name__)


# ============================================================================
# 작업 모드 정의
# ============================================================================
class WorkMode:
    """작업 모드 상수"""
    FULL = 'full'                # INSERT -> COMMIT -> SELECT (기본)
    INSERT_ONLY = 'insert-only'  # INSERT -> COMMIT만
    SELECT_ONLY = 'select-only'  # SELECT만 (기존 데이터 필요)
    UPDATE_ONLY = 'update-only'  # UPDATE만 (기존 데이터 필요)
    DELETE_ONLY = 'delete-only'  # DELETE만 (기존 데이터 필요)
    MIXED = 'mixed'              # INSERT/UPDATE/DELETE/SELECT 혼합


# ============================================================================
# 우아한 종료 핸들러
# ============================================================================
class GracefulShutdown:
    """우아한 종료(Graceful Shutdown) 핸들러"""

    def __init__(self):
        """GracefulShutdown 초기화

        SIGINT(Ctrl+C), SIGTERM 시그널 핸들러를 등록하여
        프로세스 종료 요청 시 현재 진행 중인 트랜잭션을 완료한 후 종료
        """
        self.shutdown_requested = False
        self.lock = threading.Lock()

        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """시그널 핸들러 콜백

        Args:
            signum: 수신된 시그널 번호
            frame: 현재 스택 프레임
        """
        with self.lock:
            if not self.shutdown_requested:
                self.shutdown_requested = True
                logger.info("\n[Shutdown] Graceful shutdown requested. Completing current transactions...")

    def is_shutdown_requested(self) -> bool:
        """종료 요청 여부 확인

        Returns:
            종료가 요청되었으면 True, 그렇지 않으면 False
        """
        with self.lock:
            return self.shutdown_requested

    def request_shutdown(self):
        """프로그래밍 방식으로 종료 요청

        시그널이 아닌 코드에서 직접 종료를 요청할 때 사용
        """
        with self.lock:
            self.shutdown_requested = True


# 전역 shutdown 핸들러
shutdown_handler: Optional[GracefulShutdown] = None


# ============================================================================
# 성능 카운터 (Thread-Safe) - Enhanced with Sub-second Metrics
# ============================================================================
class PerformanceCounter:
    """스레드 안전 성능 카운터 - 1초 이내 측정 지원"""

    def __init__(self, sub_second_window_ms: int = 100):
        """PerformanceCounter 초기화

        Args:
            sub_second_window_ms: 실시간 TPS 측정 윈도우 크기 (밀리초, 기본 100ms)
        """
        self.lock = threading.Lock()
        self.total_inserts = 0
        self.total_selects = 0
        self.total_updates = 0
        self.total_deletes = 0
        self.total_transactions = 0
        self.total_errors = 0
        self.verification_failures = 0
        self.connection_recreates = 0
        self.start_time = time.time()

        # 워밍업 관련
        self.warmup_end_time: Optional[float] = None
        self.post_warmup_transactions = 0
        self.post_warmup_start_time: Optional[float] = None

        # 초 미만 단위 측정
        self.sub_second_window_ms = sub_second_window_ms
        self.sub_second_window_sec = sub_second_window_ms / 1000.0
        self.recent_transactions: deque = deque()
        self.recent_lock = threading.Lock()

        # 레이턴시 측정
        self.latencies: deque = deque(maxlen=10000)
        self.latency_lock = threading.Lock()

        # 구간별 통계
        self.last_check_time = time.time()
        self.last_transactions = 0
        self.last_inserts = 0
        self.last_selects = 0
        self.last_updates = 0
        self.last_deletes = 0
        self.last_errors = 0

        # 시계열 데이터 (결과 내보내기용)
        self.time_series: List[Dict[str, Any]] = []
        self.time_series_lock = threading.Lock()

    def set_warmup_end_time(self, warmup_end_time: float):
        """워밍업 종료 시간 설정"""
        self.warmup_end_time = warmup_end_time

    def is_warmup_period(self) -> bool:
        """워밍업 기간인지 확인"""
        if self.warmup_end_time is None:
            return False
        return time.time() < self.warmup_end_time

    def has_warmup_config(self) -> bool:
        """워밍업 설정 여부 확인"""
        return self.warmup_end_time is not None

    def record_transaction(self, latency_ms: float = 0):
        """트랜잭션 완료 기록"""
        current_time = time.time()

        with self.lock:
            self.total_transactions += 1

            # 워밍업 이후 통계
            if self.warmup_end_time and current_time >= self.warmup_end_time:
                if self.post_warmup_start_time is None:
                    self.post_warmup_start_time = current_time
                self.post_warmup_transactions += 1

        with self.recent_lock:
            self.recent_transactions.append(current_time)
            cutoff = current_time - 1.0
            while self.recent_transactions and self.recent_transactions[0] < cutoff:
                self.recent_transactions.popleft()

        if latency_ms > 0:
            with self.latency_lock:
                self.latencies.append(latency_ms)

    def increment_insert(self, count: int = 1):
        """INSERT 카운터 증가

        Args:
            count: 증가시킬 값 (배치 INSERT 시 배치 크기)
        """
        with self.lock:
            self.total_inserts += count

    def increment_select(self):
        """SELECT 카운터 1 증가"""
        with self.lock:
            self.total_selects += 1

    def increment_update(self):
        """UPDATE 카운터 1 증가"""
        with self.lock:
            self.total_updates += 1

    def increment_delete(self):
        """DELETE 카운터 1 증가"""
        with self.lock:
            self.total_deletes += 1

    def increment_error(self):
        """에러 카운터 1 증가"""
        with self.lock:
            self.total_errors += 1

    def increment_verification_failure(self):
        """검증 실패 카운터 1 증가 (INSERT 후 SELECT 검증 실패 시)"""
        with self.lock:
            self.verification_failures += 1

    def increment_connection_recreate(self):
        """커넥션 재생성 카운터 1 증가 (손상된 커넥션 교체 시)"""
        with self.lock:
            self.connection_recreates += 1

    def get_sub_second_tps(self) -> float:
        """최근 1초간의 TPS (실시간 처리량)

        Returns:
            최근 1초간 완료된 트랜잭션 수
        """
        current_time = time.time()

        with self.recent_lock:
            cutoff = current_time - 1.0
            while self.recent_transactions and self.recent_transactions[0] < cutoff:
                self.recent_transactions.popleft()
            count = len(self.recent_transactions)

        return float(count)

    def get_windowed_tps(self, window_ms: Optional[int] = None) -> float:
        """지정된 윈도우 내 TPS (지정된 시간 범위의 평균 처리량)

        Args:
            window_ms: 측정 윈도우 크기 (밀리초, None이면 기본값 사용)

        Returns:
            지정된 윈도우 내 초당 트랜잭션 수
        """
        if window_ms is None:
            window_ms = self.sub_second_window_ms

        window_sec = window_ms / 1000.0
        current_time = time.time()
        cutoff = current_time - window_sec

        with self.recent_lock:
            count = sum(1 for t in self.recent_transactions if t >= cutoff)

        return count / window_sec if window_sec > 0 else 0.0

    def get_latency_stats(self) -> Dict[str, float]:
        """레이턴시 통계 조회

        Returns:
            avg, p50, p95, p99, min, max 값을 포함한 딕셔너리
        """
        with self.latency_lock:
            if not self.latencies:
                return {'avg': 0, 'p50': 0, 'p95': 0, 'p99': 0, 'min': 0, 'max': 0}

            sorted_latencies = sorted(self.latencies)
            n = len(sorted_latencies)

            return {
                'avg': sum(sorted_latencies) / n,
                'p50': sorted_latencies[int(n * 0.50)],
                'p95': sorted_latencies[int(n * 0.95)] if n > 20 else sorted_latencies[-1],
                'p99': sorted_latencies[int(n * 0.99)] if n > 100 else sorted_latencies[-1],
                'min': sorted_latencies[0],
                'max': sorted_latencies[-1]
            }

    def get_interval_stats(self) -> Dict[str, Any]:
        """이전 호출 이후의 구간별 통계 조회

        마지막 호출 시점부터 현재까지의 트랜잭션, INSERT, SELECT 등 통계를 반환
        호출 시 내부 상태가 갱신되어 다음 호출 시 새로운 구간 통계 제공

        Returns:
            interval_seconds, interval_transactions, interval_tps 등을 포함한 딕셔너리
        """
        current_time = time.time()

        with self.lock:
            interval_time = current_time - self.last_check_time
            interval_transactions = self.total_transactions - self.last_transactions
            interval_inserts = self.total_inserts - self.last_inserts
            interval_selects = self.total_selects - self.last_selects
            interval_updates = self.total_updates - self.last_updates
            interval_deletes = self.total_deletes - self.last_deletes
            interval_errors = self.total_errors - self.last_errors

            self.last_check_time = current_time
            self.last_transactions = self.total_transactions
            self.last_inserts = self.total_inserts
            self.last_selects = self.total_selects
            self.last_updates = self.total_updates
            self.last_deletes = self.total_deletes
            self.last_errors = self.total_errors

            interval_tps = interval_transactions / interval_time if interval_time > 0 else 0

            return {
                'interval_seconds': interval_time,
                'interval_transactions': interval_transactions,
                'interval_inserts': interval_inserts,
                'interval_selects': interval_selects,
                'interval_updates': interval_updates,
                'interval_deletes': interval_deletes,
                'interval_errors': interval_errors,
                'interval_tps': round(interval_tps, 2)
            }

    def record_time_series(self, pool_stats: Optional[Dict[str, Union[int, str]]] = None):
        """시계열 데이터 기록

        주기적으로 호출되어 현재 성능 지표를 시계열 리스트에 저장
        CSV/JSON 결과 내보내기 시 사용됨

        Args:
            pool_stats: 커넥션 풀 상태 딕셔너리 (옵션)
        """
        current_time = time.time()
        stats = self.get_stats()
        latency_stats = self.get_latency_stats()

        record = {
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': round(current_time - self.start_time, 2),
            'total_transactions': stats['total_transactions'],
            'total_inserts': stats['total_inserts'],
            'total_selects': stats['total_selects'],
            'total_updates': stats['total_updates'],
            'total_deletes': stats['total_deletes'],
            'total_errors': stats['total_errors'],
            'realtime_tps': stats['realtime_tps'],
            'avg_tps': stats['avg_tps'],
            'latency_avg': round(latency_stats['avg'], 2),
            'latency_p50': round(latency_stats['p50'], 2),
            'latency_p95': round(latency_stats['p95'], 2),
            'latency_p99': round(latency_stats['p99'], 2),
            'is_warmup': self.is_warmup_period()
        }

        if pool_stats:
            record.update(pool_stats)

        with self.time_series_lock:
            self.time_series.append(record)

    def get_stats(self) -> Dict[str, Any]:
        """전체 성능 통계 조회

        테스트 시작부터 현재까지의 누적 통계를 반환

        Returns:
            total_inserts, total_transactions, avg_tps, realtime_tps 등을 포함한 딕셔너리
        """
        with self.lock:
            elapsed_time = time.time() - self.start_time
            avg_tps = self.total_transactions / elapsed_time if elapsed_time > 0 else 0

            # 워밍업 후 통계
            post_warmup_tps = 0
            if self.post_warmup_start_time:
                post_warmup_elapsed = time.time() - self.post_warmup_start_time
                post_warmup_tps = self.post_warmup_transactions / post_warmup_elapsed if post_warmup_elapsed > 0 else 0

            return {
                'total_inserts': self.total_inserts,
                'total_selects': self.total_selects,
                'total_updates': self.total_updates,
                'total_deletes': self.total_deletes,
                'total_transactions': self.total_transactions,
                'total_errors': self.total_errors,
                'verification_failures': self.verification_failures,
                'connection_recreates': self.connection_recreates,
                'elapsed_seconds': elapsed_time,
                'avg_tps': round(avg_tps, 2),
                'realtime_tps': round(self.get_sub_second_tps(), 2),
                'post_warmup_transactions': self.post_warmup_transactions,
                'post_warmup_tps': round(post_warmup_tps, 2)
            }


# 전역 성능 카운터
perf_counter: Optional[PerformanceCounter] = None


# ============================================================================
# Rate Limiter (토큰 버킷 알고리즘)
# ============================================================================
class RateLimiter:
    """Token Bucket 기반 Rate Limiter (속도 제한)"""

    def __init__(self, target_tps: int):
        """RateLimiter 초기화

        Args:
            target_tps: 목표 초당 트랜잭션 수 (0 이하면 비활성화)
        """
        self.target_tps = target_tps
        self.tokens = target_tps
        self.max_tokens = target_tps * 2  # 버스트 허용
        self.last_refill = time.time()
        self.lock = threading.Lock()

        if target_tps <= 0:
            self.enabled = False
        else:
            self.enabled = True

    def acquire(self, timeout: float = 1.0) -> bool:
        """토큰 획득 시도 (속도 제한)

        Token Bucket 알고리즘을 사용하여 TPS를 제한합니다.
        토큰이 있으면 즉시 반환, 없으면 타임아웃까지 대기합니다.

        Args:
            timeout: 토큰 획득 최대 대기 시간 (초)

        Returns:
            토큰 획득 성공 시 True, 타임아웃 시 False
        """
        if not self.enabled:
            return True

        start_time = time.time()

        while True:
            with self.lock:
                # 토큰 리필
                now = time.time()
                elapsed = now - self.last_refill
                refill_amount = elapsed * self.target_tps
                self.tokens = min(self.max_tokens, self.tokens + refill_amount)
                self.last_refill = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return True

            # 타임아웃 체크
            if time.time() - start_time > timeout:
                return False

            # 짧은 대기
            time.sleep(0.001)


# ============================================================================
# 결과 내보내기
# ============================================================================
class ResultExporter:
    """테스트 결과 내보내기 유틸리티"""

    @staticmethod
    def export_csv(filepath: str, stats: Dict[str, Any], time_series: List[Dict],
                   config: Dict[str, Any]):
        """테스트 결과를 CSV 형식으로 내보내기

        Args:
            filepath: 저장할 파일 경로
            stats: 최종 성능 통계 딕셔너리
            time_series: 시계열 데이터 리스트
            config: 테스트 설정 딕셔너리
        """
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 설정 정보
            writer.writerow(['# Configuration'])
            for key, value in config.items():
                writer.writerow([f'# {key}', value])
            writer.writerow([])

            # 최종 통계
            writer.writerow(['# Final Statistics'])
            for key, value in stats.items():
                writer.writerow([f'# {key}', value])
            writer.writerow([])

            # 시계열 데이터
            if time_series:
                writer.writerow(['# Time Series Data'])
                headers = list(time_series[0].keys())
                writer.writerow(headers)
                for record in time_series:
                    writer.writerow([record.get(h, '') for h in headers])

        logger.info(f"Results exported to CSV: {filepath}")

    @staticmethod
    def export_json(filepath: str, stats: Dict[str, Any], time_series: List[Dict],
                    config: Dict[str, Any], latency_stats: Dict[str, float]):
        """테스트 결과를 JSON 형식으로 내보내기

        Args:
            filepath: 저장할 파일 경로
            stats: 최종 성능 통계 딕셔너리
            time_series: 시계열 데이터 리스트
            config: 테스트 설정 딕셔너리
            latency_stats: 레이턴시 통계 딕셔너리
        """
        result = {
            'test_info': {
                'timestamp': datetime.now().isoformat(),
                'version': '2.4'
            },
            'configuration': config,
            'final_statistics': stats,
            'latency_statistics': latency_stats,
            'time_series': time_series
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logger.info(f"Results exported to JSON: {filepath}")


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
    'singlestore': JDBCDriverInfo(
        driver_class='com.singlestore.jdbc.Driver',
        jar_pattern='singlestore-jdbc-*.jar',
        url_template='jdbc:singlestore://{host}:{port}/{database}'
    ),
    'sqlserver': JDBCDriverInfo(
        driver_class='com.microsoft.sqlserver.jdbc.SQLServerDriver',
        jar_pattern='mssql-jdbc-*.jar',
        url_template='jdbc:sqlserver://{host}:{port};databaseName={database}'
    ),
    'db2': JDBCDriverInfo(
        driver_class='com.ibm.db2.jcc.DB2Driver',
        jar_pattern='*jcc*.jar',
        url_template='jdbc:db2://{host}:{port}/{database}'
    )
}


def initialize_jvm(jre_dir: str = './jre'):
    """JVM 초기화 및 JDBC 드라이버 로드

    지정된 디렉터리의 모든 JAR 파일을 클래스패스에 추가하고 JVM을 시작합니다.
    이미 JVM이 시작된 경우에는 아무 동작도 하지 않습니다.

    Args:
        jre_dir: JDBC 드라이버 JAR 파일이 있는 디렉터리 경로
    """
    if jpype.isJVMStarted():
        return

    jvm_path = jpype.getDefaultJVMPath()
    logger.info(f"Initializing JVM using: {jvm_path}")

    jars = []
    for root, dirs, files in os.walk(jre_dir):
        for file in files:
            if file.endswith('.jar'):
                jars.append(os.path.join(root, file))

    classpath = os.pathsep.join(jars) or "."
    logger.info(f"JVM Classpath: {classpath}")

    jvm_args = [
        f"-Djava.class.path={classpath}",
        "-Dfile.encoding=UTF-8",
        "-Xms512m",
        "-Xmx2048m",
        "-XX:+UseSerialGC"
    ]

    try:
        jpype.startJVM(jvm_path, *jvm_args)
        logger.info("JVM initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize JVM: {e}")
        sys.exit(1)


def find_jdbc_jar(db_type: str, jre_dir: str = './jre') -> Optional[str]:
    """데이터베이스 타입에 맞는 JDBC JAR 파일 찾기

    DB별 서브디렉터리(예: ./jre/oracle/)를 먼저 검색하고,
    없으면 전체 디렉터리를 재귀 검색합니다.

    Args:
        db_type: 데이터베이스 타입 (oracle, postgresql, mysql 등)
        jre_dir: JDBC 드라이버 JAR 파일이 있는 디렉터리 경로

    Returns:
        JDBC JAR 파일 경로, 찾지 못한 경우 None

    Raises:
        ValueError: 지원하지 않는 DB 타입인 경우
    """
    if db_type not in JDBC_DRIVERS:
        raise ValueError(f"Unsupported DB type: {db_type}")

    driver_info = JDBC_DRIVERS[db_type]

    db_subdir = os.path.join(jre_dir, db_type)
    if os.path.exists(db_subdir):
        pattern = os.path.join(db_subdir, driver_info.jar_pattern)
        jar_files = glob.glob(pattern)
        if jar_files:
            jar_file = sorted(jar_files)[-1]
            logger.info(f"Found JDBC driver: {jar_file}")
            return jar_file

    pattern = os.path.join(jre_dir, '**', driver_info.jar_pattern)
    jar_files = glob.glob(pattern, recursive=True)

    if not jar_files:
        logger.error(f"JDBC driver not found: {driver_info.jar_pattern} in {jre_dir}")
        return None

    jar_file = sorted(jar_files)[-1]
    logger.info(f"Found JDBC driver: {jar_file}")
    return jar_file


# ============================================================================
# 커넥션 풀 (Connection Pool) - Enhanced with Monitoring, Leak Detection, Health Check
# ============================================================================
@dataclass
class PooledConnection:
    """풀링된 커넥션 래퍼 - 생성 시간 및 획득 시간 추적"""
    connection: Any
    created_at: float = field(default_factory=time.time)
    acquired_at: Optional[float] = None
    acquired_by: Optional[str] = None
    last_used_at: float = field(default_factory=time.time)

    def mark_acquired(self, thread_name: Optional[str] = None):
        """커넥션 획득 시 호출

        Leak 감지를 위해 획득 시간 및 스레드 정보를 기록합니다.

        Args:
            thread_name: 커넥션을 획득한 스레드 이름 (옵션)
        """
        self.acquired_at = time.time()
        self.acquired_by = thread_name or threading.current_thread().name
        self.last_used_at = self.acquired_at

    def mark_released(self):
        """커넥션 반환 시 호출

        획득 정보를 초기화하고 마지막 사용 시간을 갱신합니다.
        """
        self.acquired_at = None
        self.acquired_by = None
        self.last_used_at = time.time()

    def get_age_seconds(self) -> float:
        """커넥션 생성 후 경과 시간 (초)

        Max Lifetime 검사에 사용됩니다.

        Returns:
            커넥션 생성 후 경과한 초
        """
        return time.time() - self.created_at

    def get_acquired_duration_seconds(self) -> Optional[float]:
        """커넥션 획득 후 경과 시간 (초)

        Leak 감지에 사용됩니다. 임계값을 초과하면 경고를 발생시킵니다.

        Returns:
            획득 후 경과한 초, 미획득 상태면 None
        """
        if self.acquired_at is None:
            return None
        return time.time() - self.acquired_at

    def get_idle_seconds(self) -> Optional[float]:
        """커넥션 유휴 시간 (초)

        유휴 커넥션 Health Check에 사용됩니다.

        Returns:
            유휴 시간 (초), 사용 중이면 None
        """
        if self.acquired_at is not None:
            return None
        return time.time() - self.last_used_at


class JDBCConnectionPool:
    """JDBC 커넥션 풀 - 모니터링, Leak 감지, Health Check 지원

    주요 기능:
    - Connection Leak 감지: 오래 사용 중인 커넥션 추적 및 경고
    - Pool Warm-up: 초기화 시 min_size만큼 커넥션 미리 생성
    - Max Lifetime: 오래된 커넥션 자동 갱신
    - Idle Health Check: 유휴 커넥션 주기적 검증
    """

    def __init__(self, jdbc_url: str, driver_class: str, jar_file: str,
                 user: str, password: str, min_size: int, max_size: int,
                 validation_timeout: int = 5,
                 max_lifetime_seconds: int = 1800,
                 leak_detection_threshold_seconds: int = 60,
                 idle_check_interval_seconds: int = 30,
                 idle_timeout_seconds: int = 30,
                 keepalive_time_seconds: int = 30,
                 connection_properties: Optional[Dict[str, str]] = None):
        """
        Args:
            jdbc_url: JDBC 연결 URL
            driver_class: JDBC 드라이버 클래스명
            jar_file: JDBC JAR 파일 경로
            user: 데이터베이스 사용자
            password: 데이터베이스 비밀번호
            min_size: 최소 풀 크기 (Warm-up 시 생성)
            max_size: 최대 풀 크기
            validation_timeout: 커넥션 유효성 검사 타임아웃 (초)
            max_lifetime_seconds: 커넥션 최대 수명 (초, 기본 30분)
            leak_detection_threshold_seconds: Leak 감지 임계값 (초, 기본 60초)
            idle_check_interval_seconds: 유휴 커넥션 검사 주기 (초, 기본 30초)
            connection_properties: JDBC 연결 속성 (옵션)
        """
        self.jdbc_url = jdbc_url
        self.driver_class = driver_class
        self.jar_file = jar_file
        self.user = user
        self.password = password
        self.min_size = min_size
        self.max_size = max_size
        self.validation_timeout = validation_timeout
        self.max_lifetime_seconds = max_lifetime_seconds
        self.leak_detection_threshold_seconds = leak_detection_threshold_seconds
        self.idle_check_interval_seconds = idle_check_interval_seconds
        self.idle_timeout_seconds = idle_timeout_seconds
        
        # 연결 속성 설정 (user/password 포함)
        self.connection_properties = connection_properties.copy() if connection_properties else {}
        self.connection_properties['user'] = user
        self.connection_properties['password'] = password

        if keepalive_time_seconds > 0 and keepalive_time_seconds < 30:
            logger.warning("Keepalive time < 30s; disabling keepalive checks")
            self.keepalive_time_seconds = 0
        else:
            self.keepalive_time_seconds = keepalive_time_seconds

        self.pool = queue.Queue(maxsize=max_size)
        self.current_size = 0
        self.active_count = 0
        self.lock = threading.Lock()

        # Leak 감지용: 현재 사용 중인 커넥션 추적 (conn_id -> PooledConnection)
        self.active_connections: Dict[int, PooledConnection] = {}
        self.active_connections_lock = threading.Lock()

        # 통계
        self.total_created = 0
        self.total_recycled = 0  # max_lifetime 초과로 재생성된 커넥션 수
        self.total_leaked_warnings = 0

        # Health Check 스레드 관리
        self._health_check_thread: Optional[threading.Thread] = None
        self._health_check_running = False



        logger.info(f"Initializing JDBC connection pool (min={min_size}, max={max_size})")
        logger.info(f"  - Max Lifetime: {max_lifetime_seconds}s")
        logger.info(f"  - Leak Detection Threshold: {leak_detection_threshold_seconds}s")
        logger.info(f"  - Idle Check Interval: {idle_check_interval_seconds}s")
        logger.info(f"  - Idle Timeout: {self.idle_timeout_seconds}s")
        logger.info(f"  - Keepalive Time: {self.keepalive_time_seconds}s")
        logger.info(f"JDBC URL: {jdbc_url}")

        # 풀 웜업: min_size만큼 커넥션 미리 생성
        self._warmup_pool()

    def _warmup_pool(self):
        """풀 웜업: 초기화 시 min_size만큼 커넥션 미리 생성

        테스트 시작 전에 커넥션을 미리 생성하여 초기 지연을 방지합니다.
        Health Check 스레드도 함께 시작됩니다.
        """
        logger.info(f"[Pool Warm-up] Creating {self.min_size} initial connections...")
        created = 0
        for i in range(self.min_size):
            try:
                pooled_conn = self._create_connection_internal()
                if pooled_conn:
                    self.pool.put(pooled_conn)
                    created += 1
            except Exception as e:
                logger.warning(f"[Pool Warm-up] Failed to create connection {i+1}: {e}")
        logger.info(f"[Pool Warm-up] Completed. Created {created}/{self.min_size} connections")

        # Health Check 스레드 시작 (유휴 커넥션 검사 및 Leak 감지)
        self._start_health_check_thread()

    def _create_connection_internal(self) -> Optional[PooledConnection]:
        """
        내부용 커넥션 생성 (재시도 로직 포함)

        개선사항:
        1. DB 재기동 등 일시적 연결 실패 시 최대 3회 재시도
        2. Exponential Backoff 적용 (100ms → 200ms → 400ms, 최대 2초)
        3. 상세 로깅으로 실패 원인 추적 용이
        4. DB listener 과도 부하 방지를 위한 지수적 대기

        Returns:
            성공 시 PooledConnection 객체, 실패 시 None
        """
        max_creation_retries = 3  # 최대 재시도 횟수
        creation_backoff_ms = 100  # 초기 백오프 시간 (밀리초)

        for attempt in range(max_creation_retries):
            # 커넥션 풀 용량 체크 (최대 크기 초과 시 생성 불가)
            with self.lock:
                if self.current_size >= self.max_size:
                    return None
                self.current_size += 1  # 생성 시도 전에 카운트 증가

            try:
                # 커넥션 생성 (Properties에 타임아웃 등 설정 포함)
                conn = jaydebeapi.connect(
                    self.driver_class,
                    self.jdbc_url,
                    self.connection_properties,
                    self.jar_file
                )
                conn.jconn.setAutoCommit(False)

                # 네트워크 타임아웃 명시적 설정 (JDBC 4.1+ 지원 시)
                try:
                    timeout_ms = 5000  # 기본 5초 타임아웃
                    if 'oracle.jdbc.ReadTimeout' in self.connection_properties:
                         timeout_ms = int(self.connection_properties['oracle.jdbc.ReadTimeout'])

                    if hasattr(conn.jconn, 'setNetworkTimeout'):
                         conn.jconn.setNetworkTimeout(None, timeout_ms)
                except Exception:
                    pass

                # 커넥션 생성 성공: 카운터 증가 및 PooledConnection 래핑 반환
                with self.lock:
                    self.total_created += 1

                return PooledConnection(connection=conn)

            except Exception as e:
                # 생성 실패: 카운터 감소
                with self.lock:
                    self.current_size -= 1

                # 마지막 시도가 아닌 경우: 재시도 수행
                if attempt < max_creation_retries - 1:
                    logger.warning(
                        f"[Connection Creation] Attempt {attempt + 1}/{max_creation_retries} failed: {e}. "
                        f"Retrying in {creation_backoff_ms}ms..."
                    )
                    # 지수적 백오프: 100ms → 200ms → 400ms → 800ms → 1600ms → 2000ms(최대)
                    time.sleep(creation_backoff_ms / 1000.0)
                    creation_backoff_ms = min(creation_backoff_ms * 2, 2000)
                else:
                    # 최대 재시도 초과: 최종 에러 로그
                    logger.error(
                        f"[Connection Creation] Failed after {max_creation_retries} attempts (URL: {self.jdbc_url}): {e}"
                    )
                return None

        return None

    def _create_connection(self):
        """새 커넥션 생성 (하위 호환성 유지)

        _create_connection_internal을 호출하고 생성된 커넥션을 풀에 추가합니다.

        Returns:
            생성된 커넥션 객체, 실패 시 None
        """
        pooled_conn = self._create_connection_internal()
        if pooled_conn:
            self.pool.put(pooled_conn)
            return pooled_conn.connection
        return None

    def _validate_connection(self, conn) -> bool:
        """커넥션 유효성 검증

        JDBC isValid() 메서드를 사용하여 커넥션이 유효한지 확인합니다.

        Args:
            conn: 검증할 커넥션 (PooledConnection 또는 raw connection)

        Returns:
            유효하면 True, 그렇지 않으면 False
        """
        try:
            if conn is None:
                return False
            # PooledConnection 래퍼인 경우 내부 connection 추출
            actual_conn = conn.connection if isinstance(conn, PooledConnection) else conn
            jconn = actual_conn.jconn
            if jconn.isClosed():
                return False
            if hasattr(jconn, 'isValid'):
                return jconn.isValid(self.validation_timeout)
            return True
        except Exception:
            return False

    def _is_connection_expired(self, pooled_conn: PooledConnection) -> bool:
        """커넥션이 max_lifetime을 초과했는지 확인

        Args:
            pooled_conn: 검사할 PooledConnection

        Returns:
            만료되었으면 True, 그렇지 않으면 False
        """
        return pooled_conn.get_age_seconds() > self.max_lifetime_seconds

    def _start_health_check_thread(self):
        """Health Check 스레드 시작

        유휴 커넥션 검증 및 Leak 감지를 위한 백그라운드 스레드를 시작합니다.
        """
        if self._health_check_thread is not None and self._health_check_thread.is_alive():
            return

        self._health_check_running = True
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            name="PoolHealthCheck",
            daemon=True
        )
        self._health_check_thread.start()
        logger.info("[Health Check] Thread started")

    def _health_check_loop(self):
        """Health Check 메인 루프

        주기적으로 유휴 커넥션 검증 및 Leak 감지를 수행합니다.
        풀 종료 시 자동으로 중단됩니다.
        """
        while self._health_check_running:
            try:
                time.sleep(self.idle_check_interval_seconds)
                if not self._health_check_running:
                    break

                self._check_idle_connections()
                self._detect_connection_leaks()
            except Exception as e:
                logger.error(f"[Health Check] Error: {e}")

    def _check_idle_connections(self):
        """유휴 커넥션 Health Check 및 정리

        풀의 모든 유휴 커넥션을 검사하여:
        1. Max Lifetime 초과 커넥션 재생성
        2. Idle Timeout 초과 커넥션 제거 (min_size 유지)
        3. Keepalive 시간 초과 커넥션 유효성 검사
        4. 무효한 커넥션 제거 및 min_size 유지를 위한 새 커넥션 생성
        """
        # 검사 통계 카운터 초기화
        checked = 0   # 검사한 커넥션 수
        removed = 0   # 제거된 커넥션 수
        recycled = 0  # 재생성된 커넥션 수

        # 유효한 커넥션을 임시 저장할 리스트
        valid_connections = []

        try:
            # 풀에서 모든 커넥션을 꺼내서 검사
            while True:
                try:
                    # 큐에서 커넥션을 논블로킹으로 꺼냄
                    pooled_conn = self.pool.get_nowait()
                except queue.Empty:
                    # 큐가 비었으면 검사 루프 종료
                    break

                checked += 1

                # [검사 1] Max Lifetime 초과 여부 확인
                if self._is_connection_expired(pooled_conn):
                    # 만료된 커넥션은 닫고 새로 생성
                    self._close_pooled_connection(pooled_conn)
                    recycled += 1
                    with self.lock:
                        self.total_recycled += 1
                    # 새 커넥션 생성하여 유효 목록에 추가
                    new_conn = self._create_connection_internal()
                    if new_conn:
                        valid_connections.append(new_conn)
                    continue

                # 유휴 시간 계산
                idle_seconds = pooled_conn.get_idle_seconds()

                # [검사 2] Idle Timeout 초과 여부 확인
                if idle_seconds is not None and self.idle_timeout_seconds > 0:
                    with self.lock:
                        # min_size 이상일 때만 제거 가능
                        can_drop = self.current_size > self.min_size
                    if can_drop and idle_seconds > self.idle_timeout_seconds:
                        # 오래 유휴 상태인 커넥션 제거 (풀 축소)
                        self._close_pooled_connection(pooled_conn)
                        removed += 1
                        continue

                # [검사 3] Keepalive 시간 초과 시 유효성 검사
                keepalive_checked = False
                if idle_seconds is not None and self.keepalive_time_seconds > 0:
                    if idle_seconds > self.keepalive_time_seconds:
                        # keepalive 시간 초과 - 유효성 검사 수행
                        keepalive_checked = True
                        if not self._validate_connection(pooled_conn):
                            # 유효하지 않은 커넥션은 닫고 새로 생성
                            self._close_pooled_connection(pooled_conn)
                            removed += 1
                            new_conn = self._create_connection_internal()
                            if new_conn:
                                valid_connections.append(new_conn)
                            continue
                        # 유효성 검사 통과 시 마지막 사용 시간 갱신
                        pooled_conn.last_used_at = time.time()

                # [검사 4] 최종 유효성 판정
                if keepalive_checked or self._validate_connection(pooled_conn):
                    # 유효한 커넥션은 보존
                    valid_connections.append(pooled_conn)
                else:
                    # 무효한 커넥션은 제거
                    self._close_pooled_connection(pooled_conn)
                    removed += 1

        finally:
            # 유효한 커넥션들을 다시 풀에 반환
            for conn in valid_connections:
                try:
                    self.pool.put_nowait(conn)
                except queue.Full:
                    # 풀이 가득 차면 커넥션 닫기
                    self._close_pooled_connection(conn)

        # min_size 유지: 부족한 커넥션 보충
        while True:
            with self.lock:
                if self.current_size >= self.min_size:
                    # 최소 크기 충족 시 종료
                    break
            # 새 커넥션 생성
            new_conn = self._create_connection_internal()
            if not new_conn:
                # 생성 실패 시 종료
                break
            try:
                self.pool.put_nowait(new_conn)
            except queue.Full:
                # 풀이 가득 차면 커넥션 닫고 종료
                self._close_pooled_connection(new_conn)
                break

        # 변경사항이 있을 경우 로그 출력
        if removed > 0 or recycled > 0:
            logger.info(f"[Health Check] Checked: {checked}, Removed: {removed}, Recycled: {recycled}")

    def _detect_connection_leaks(self):
        """커넥션 Leak 감지

        획득된 상태로 임계 시간을 초과한 커넥션을 탐지하여 경고 로그를 출력합니다.
        Leak된 커넥션은 자동으로 회수되지 않으며, 경고만 발생시킵니다.
        """
        # Leak 의심 커넥션 목록
        leaked_connections = []

        # 활성 커넥션들을 순회하며 Leak 여부 확인
        with self.active_connections_lock:
            for conn_id, pooled_conn in self.active_connections.items():
                # 커넥션이 획득된 후 경과 시간 계산
                duration = pooled_conn.get_acquired_duration_seconds()
                # 임계 시간 초과 시 Leak으로 판정
                if duration and duration > self.leak_detection_threshold_seconds:
                    leaked_connections.append({
                        'conn_id': conn_id,
                        'duration': duration,
                        'thread': pooled_conn.acquired_by
                    })

        # Leak 감지된 커넥션들에 대해 경고 로그 출력
        for leak in leaked_connections:
            self.total_leaked_warnings += 1
            logger.warning(
                f"[Leak Detection] Potential connection leak detected! "
                f"Connection held for {leak['duration']:.1f}s by thread '{leak['thread']}' "
                f"(threshold: {self.leak_detection_threshold_seconds}s)"
            )

    def _close_pooled_connection(self, pooled_conn: PooledConnection):
        """PooledConnection 종료 및 풀 크기 감소

        Args:
            pooled_conn: 종료할 PooledConnection
        """
        try:
            # 커넥션이 존재하면 닫기
            if pooled_conn and pooled_conn.connection:
                pooled_conn.connection.close()
        except:
            # 닫기 실패 시 무시 (이미 닫혔거나 오류 상태)
            pass
        # 풀 크기 감소 (음수 방지)
        with self.lock:
            self.current_size = max(0, self.current_size - 1)

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        """풀 상태 조회 (Non-blocking)

        모니터링 스레드가 락 대기로 멈추지 않도록 타임아웃을 적용합니다.

        Returns:
            pool_total, pool_active, pool_idle 등을 포함한 딕셔너리
        """
        # 락 획득 시도 (0.1초 타임아웃) - 모니터링 스레드가 멈추지 않도록 함
        if not self.lock.acquire(timeout=0.1):
            # 락 획득 실패 시 현재 값을 그대로 반환 (락 없이 접근은 안전하지 않지만 멈추는 것보다 나음)
            return {
                'pool_total': self.current_size,
                'pool_active': self.active_count,
                'pool_idle': max(0, self.current_size - self.active_count),
                'pool_total_created': self.total_created,
                'pool_recycled': self.total_recycled,
                'pool_leak_warnings': self.total_leaked_warnings,
                'status': 'locked'  # 락 획득 실패 상태 표시
            }

        try:
            # 락 획득 성공 시 정확한 풀 상태 반환
            return {
                'pool_total': self.current_size,
                'pool_active': self.active_count,
                'pool_idle': self.current_size - self.active_count,
                'pool_total_created': self.total_created,
                'pool_recycled': self.total_recycled,
                'pool_leak_warnings': self.total_leaked_warnings
            }
        finally:
            # 락 해제 (반드시 실행)
            self.lock.release()

    def acquire(self, timeout: int = 30):
        """
        커넥션 획득 (향상된 재시도 로직 포함)

        개선사항:
        1. DB 재기동 시 커넥션 풀 복구 로직 강화
        2. Exponential Backoff 적용 (100ms → 200ms → 400ms → 800ms → 1600ms → 5000ms)
        3. 큐가 비어있을 때만 새 커넥션 생성 (DB listener 부하 감소)
        4. Max Lifetime 초과 시 자동 재생성
        5. 상세 로깅으로 재시도 추적

        Args:
            timeout: 커넥션 획득 최대 대기 시간 (초)

        Returns:
            연결된 커넥션 객체 (성공), None (실패)
        """
        retry_count = 0
        max_retries = 3  # 최대 재시도 횟수
        backoff_ms = 100  # 초기 백오프 시간 (밀리초)
        thread_name = threading.current_thread().name

        while retry_count < max_retries:
            try:
                # 큐 대기 시간 결정:
                # - 큐가 비어있고 풀에 여유가 있으면: 빠르게 생성 시도 (0.1초)
                # - 그 외: 전체 타임아웃까지 대기
                wait_time = timeout
                if self.current_size < self.max_size and self.pool.empty():
                     wait_time = 0.1  # 빠른 실패로 'except Empty' 분기에서 새 커넥션 생성

                # 큐에서 커넥션 획득 시도
                pooled_conn = self.pool.get(timeout=wait_time)

                # 최대 수명 초과 시 재생성 (오래된 커넥션 자동 교체)
                if self._is_connection_expired(pooled_conn):
                    self._close_pooled_connection(pooled_conn)
                    with self.lock:
                        self.total_recycled += 1
                    pooled_conn = self._create_connection_internal()

                    if pooled_conn is None:
                        # 새 커넥션 생성 실패: 재시도 카운트 증가 및 백오프 적용
                        retry_count += 1
                        time.sleep(backoff_ms / 1000.0)
                        backoff_ms = min(backoff_ms * 2, 5000)  # 지수적 백오프
                        continue

                # 커넥션 유효성 검사 (Closed 검증)
                if self._validate_connection(pooled_conn):
                    pooled_conn.mark_acquired(thread_name)

                    # Leak 감지용 추적: 현재 사용 중인 커넥션 등록
                    conn_id = id(pooled_conn.connection)
                    with self.active_connections_lock:
                        self.active_connections[conn_id] = pooled_conn

                    # 활성 커넥션 카운트 증가
                    with self.lock:
                        self.active_count += 1

                    return pooled_conn.connection
                else:
                    # 유효하지 않은 커넥션: 폐기
                    self._close_pooled_connection(pooled_conn)
                    # 유효성 검사 실패 시 루프에서 재시도하도록 함
                    pass

            except queue.Empty:
                # 큐가 비어있음: 새 커넥션 생성 시도
                can_create = False
                with self.lock:
                    can_create = self.current_size < self.max_size

                if can_create:
                    # 새 커넥션 생성 시도 (내부에서 재시도 로직 실행됨)
                    pooled_conn = self._create_connection_internal()
                    if pooled_conn:
                        # 커넥션 생성 성공: 반환
                        pooled_conn.mark_acquired(thread_name)
                        conn_id = id(pooled_conn.connection)
                        with self.active_connections_lock:
                            self.active_connections[conn_id] = pooled_conn
                        with self.lock:
                            self.active_count += 1
                        return pooled_conn.connection
                    # 커넥션 생성 실패: 로그 기록
                    logger.warning(
                        f"[acquire] Connection creation failed "
                        f"(attempt {retry_count + 1}/{max_retries})"
                    )

                # 백오프 후 재시도 (DB 리스너 과부하 방지)
                if retry_count < max_retries - 1:
                    time.sleep(backoff_ms / 1000.0)
                    backoff_ms = min(backoff_ms * 2, 5000)  # 지수적 백오프
                retry_count += 1

        # 최대 재시도 후에도 실패: 최종 시도
        try:
            # 풀에서 커넥션 획득 시도 (timeout 시간 동안 대기)
            pooled_conn = self.pool.get(timeout=timeout)
            if pooled_conn:
                # 커넥션 획득 성공: 획득 상태로 표시
                pooled_conn.mark_acquired(thread_name)
                conn_id = id(pooled_conn.connection)
                # 활성 커넥션 목록에 추가
                with self.active_connections_lock:
                    self.active_connections[conn_id] = pooled_conn
                # 활성 커넥션 수 증가
                with self.lock:
                    self.active_count += 1
                return pooled_conn.connection
        except queue.Empty:
            # 풀이 비어있어 커넥션 획득 실패
            logger.error(
                f"[acquire] Failed to acquire connection after {max_retries} retries "
                f"(queue empty)"
            )

        return None

    def release(self, conn):
        """커넥션 반환

        사용 완료된 커넥션을 풀에 반환합니다.
        Max Lifetime 초과 시 자동으로 재생성하고, 무효한 커넥션은 폐기합니다.

        Args:
            conn: 반환할 커넥션
        """
        if conn is None:
            return

        conn_id = id(conn)

        # Leak 감지 추적에서 제거 및 PooledConnection 복구
        pooled_conn = None
        with self.active_connections_lock:
            pooled_conn = self.active_connections.pop(conn_id, None)

        with self.lock:
            self.active_count = max(0, self.active_count - 1)

        if pooled_conn is None:
            # PooledConnection을 찾지 못한 경우 (하위 호환성 처리)
            pooled_conn = PooledConnection(connection=conn)

        pooled_conn.mark_released()

        try:
            # 최대 수명 초과 검사
            if self._is_connection_expired(pooled_conn):
                # 수명 초과된 커넥션 종료
                self._close_pooled_connection(pooled_conn)
                # 재활용 카운트 증가
                with self.lock:
                    self.total_recycled += 1
                # 새 커넥션 생성하여 풀에 추가
                new_conn = self._create_connection_internal()
                if new_conn:
                    # 새 커넥션 생성 성공 시 풀에 즉시 추가
                    self.pool.put_nowait(new_conn)
                return

            # 커넥션 유효성 검사
            if self._validate_connection(pooled_conn):
                # 풀 크기가 최대 크기 미만인 경우에만 반환
                if self.pool.qsize() < self.max_size:
                    # 유효한 커넥션을 풀에 반환
                    self.pool.put_nowait(pooled_conn)
                    return

            # 유효성 검사 실패 시 커넥션 종료
            self._close_pooled_connection(pooled_conn)

        except queue.Full:
            # 풀이 가득 찬 경우 커넥션 종료
            self._close_pooled_connection(pooled_conn)

    def discard(self, conn):
        """커넥션 폐기 (풀에 반환하지 않고 종료)

        에러 발생 등으로 인해 재사용이 불가능한 커넥션을 폐기합니다.

        Args:
            conn: 폐기할 커넥션
        """
        if conn is None:
            return

        conn_id = id(conn)

        # Leak 감지 추적에서 제거
        with self.active_connections_lock:
            self.active_connections.pop(conn_id, None)

        with self.lock:
            self.active_count = max(0, self.active_count - 1)

        try:
            conn.close()
        except:
            pass

        with self.lock:
            self.current_size = max(0, self.current_size - 1)

    def close_all(self):
        """모든 커넥션 종료 및 풀 정리

        테스트 종료 시 호출되어 Health Check 스레드를 중지하고
        풀과 활성 커넥션을 모두 정리합니다.
        """
        logger.info("Closing all connections in pool...")

        # Health Check 스레드 중지 신호
        self._health_check_running = False
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=5)

        # 풀의 모든 커넥션 종료
        while not self.pool.empty():
            try:
                pooled_conn = self.pool.get_nowait()
                self._close_pooled_connection(pooled_conn)
            except:
                pass

        # 활성 커넥션 정리
        with self.active_connections_lock:
            for conn_id, pooled_conn in list(self.active_connections.items()):
                try:
                    pooled_conn.connection.close()
                except:
                    pass
            self.active_connections.clear()

        logger.info("All connections closed")


# ============================================================================
# 데이터베이스 어댑터 인터페이스
# ============================================================================
class DatabaseAdapter(ABC):
    """데이터베이스 공통 인터페이스

    각 데이터베이스별 어댑터가 구현해야 하는 추상 메서드들을 정의합니다.
    Oracle, PostgreSQL, MySQL, SQL Server, Tibero, DB2 등을 지원합니다.
    """

    def __init__(self):
        """DatabaseAdapter 기본 초기화"""
        self.validation_timeout = 2

    @abstractmethod
    def create_connection_pool(self, config: 'DatabaseConfig'):
        """커넥션 풀 생성"""
        pass

    @abstractmethod
    def get_connection(self):
        """풀에서 커넥션 획득"""
        pass

    @abstractmethod
    def release_connection(self, connection, is_error: bool = False):
        """커넥션 반환 (에러 시 롤백 후 반환)"""
        pass

    @abstractmethod
    def discard_connection(self, connection):
        """손상된 커넥션 폐기"""
        pass

    @abstractmethod
    def close_pool(self):
        """커넥션 풀 종료"""
        pass

    @abstractmethod
    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        """커넥션 풀 상태 조회"""
        pass

    @abstractmethod
    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        """단일 INSERT 실행 및 생성된 ID 반환"""
        pass

    @abstractmethod
    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행 및 삽입 건수 반환"""
        pass

    @abstractmethod
    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """ID로 레코드 조회"""
        pass

    @abstractmethod
    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회"""
        pass

    @abstractmethod
    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행"""
        pass

    @abstractmethod
    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행"""
        pass

    @abstractmethod
    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회"""
        pass

    @abstractmethod
    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성"""
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
        """테이블 생성 DDL 반환"""
        pass

    @abstractmethod
    def setup_schema(self, connection):
        """테스트 스키마 생성"""
        pass

    @abstractmethod
    def truncate_table(self, connection):
        """테이블 데이터 삭제 (TRUNCATE)"""
        pass


# ============================================================================
# Oracle JDBC 어댑터
# ============================================================================
class OracleJDBCAdapter(DatabaseAdapter):
    """Oracle JDBC 어댑터

    Oracle 데이터베이스에 JDBC를 통해 연결하고 SQL을 실행합니다.
    SID 또는 Service Name 연결 방식을 모두 지원합니다.
    """

    def __init__(self, jre_dir: str = './jre'):
        """OracleJDBCAdapter 초기화

        Args:
            jre_dir: JDBC 드라이버 JAR 파일이 있는 디렉터리

        Raises:
            RuntimeError: Oracle JDBC 드라이버를 찾을 수 없는 경우
        """
        self.pool: Optional[JDBCConnectionPool] = None
        jar_file = find_jdbc_jar('oracle', jre_dir)
        if not jar_file:
            raise RuntimeError("Oracle JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        if config.service_name:
            jdbc_url = (
                f"jdbc:oracle:thin:@//{config.host}:{config.port or 1521}/{config.service_name}"
            )
        else:
            sid = config.sid or config.database
            if not sid:
                raise RuntimeError('Oracle SID or service name is required')
            jdbc_url = JDBC_DRIVERS['oracle'].url_template.format(
                host=config.host,
                port=config.port or 1521,
                sid=sid
            )

        # Oracle 커넥션 속성 설정
        connection_props = {}
        if config.connection_timeout_seconds > 0:
            timeout_ms = str(config.connection_timeout_seconds * 1000)
            connection_props['oracle.net.CONNECT_TIMEOUT'] = timeout_ms
            connection_props['oracle.jdbc.ReadTimeout'] = timeout_ms
            logger.info(f"Setting Oracle connection timeouts to {config.connection_timeout_seconds}s")

        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url,
            driver_class=JDBC_DRIVERS['oracle'].driver_class,
            jar_file=self.jar_file,
            user=config.user,
            password=config.password,
            min_size=config.min_pool_size,
            max_size=config.max_pool_size,
            validation_timeout=config.connection_timeout_seconds, # 유효성 검사에도 타임아웃 적용
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds,
            connection_properties=connection_props
        )
        self.validation_timeout = config.connection_timeout_seconds
        return self.pool

    def get_connection(self):
        # 풀이 비었거나 DB가 다운된 경우 빠르게 실패하도록 짧은 타임아웃 사용
        # 시도당 최대 validation_timeout(3초) 대기, 최대 3회 시도 = 9초
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        conn = self.pool.acquire(timeout=self.validation_timeout if self.validation_timeout > 0 else 5)
        if conn is None:
             logger.debug("OracleJDBCAdapter: Failed to acquire connection from pool (Timeout/Empty)")
        return conn

    def release_connection(self, connection, is_error: bool = False):
        if connection and self.pool:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except:
                pass

    def discard_connection(self, connection):
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        cursor.execute("""
            INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
            VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, SYSTIMESTAMP)
        """, [thread_id, f'TEST_{thread_id}', random_data])

        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        result = cursor.fetchone()
        return int(result[0])

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행"""
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))

        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
                VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, SYSTIMESTAMP)
            """, [thread_id, f'TEST_{thread_id}', random_data])

        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [record_id])
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        if max_id <= 0:
            return None
        random_id = random.randint(1, max_id)
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [random_id])
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        cursor.execute("""
            UPDATE LOAD_TEST SET VALUE_COL = ?, UPDATED_AT = SYSTIMESTAMP WHERE ID = ?
        """, [f'UPDATED_{record_id}', record_id])
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        cursor.execute("DELETE FROM LOAD_TEST WHERE ID = ?", [record_id])
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        cursor.execute("SELECT NVL(MAX(ID), 0) FROM LOAD_TEST")
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        if max_id <= 0:
            return 0
        return random.randint(1, max_id)

    def commit(self, connection):
        connection.commit()

    def rollback(self, connection):
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        return """
-- Oracle DDL
CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE ORDER;
CREATE TABLE LOAD_TEST (
    ID           NUMBER(19)      NOT NULL,
    THREAD_ID    VARCHAR2(50)    NOT NULL,
    VALUE_COL    VARCHAR2(200),
    RANDOM_DATA  VARCHAR2(1000),
    STATUS       VARCHAR2(20)    DEFAULT 'ACTIVE',
    CREATED_AT   TIMESTAMP       DEFAULT SYSTIMESTAMP,
    UPDATED_AT   TIMESTAMP       DEFAULT SYSTIMESTAMP
)
PARTITION BY HASH (ID) PARTITIONS 16
TABLESPACE USERS
ENABLE ROW MOVEMENT;
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);
CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL;
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            table_exists = False
            seq_exists = False

            cursor.execute("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'LOAD_TEST'")
            result = cursor.fetchone()
            if result and result[0] > 0:
                table_exists = True

            cursor.execute("SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'LOAD_TEST_SEQ'")
            result = cursor.fetchone()
            if result and result[0] > 0:
                seq_exists = True

            if table_exists and seq_exists:
                logger.info("Oracle schema already exists - reusing existing schema")
                logger.info("  (DROP objects manually to recreate, or use --truncate to clear data only)")
                return

            if seq_exists:
                try:
                    cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
                except:
                    pass
            if table_exists:
                try:
                    cursor.execute("DROP TABLE LOAD_TEST PURGE")
                except:
                    pass

            cursor.execute("CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE ORDER")
            cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID NUMBER(19) NOT NULL, THREAD_ID VARCHAR2(50) NOT NULL,
                    VALUE_COL VARCHAR2(200), RANDOM_DATA VARCHAR2(1000),
                    STATUS VARCHAR2(20) DEFAULT 'ACTIVE',
                    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                    UPDATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
                ) PARTITION BY HASH (ID) PARTITIONS 16 TABLESPACE USERS ENABLE ROW MOVEMENT
            """)
            cursor.execute("ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID)")
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL")
            connection.commit()
            logger.info("Oracle schema created successfully")
        except Exception as e:
            logger.error(f"Failed to setup Oracle schema: {e}")
            raise
        finally:
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 시퀀스 초기화

        LOAD_TEST 테이블의 모든 데이터를 삭제하고
        LOAD_TEST_SEQ 시퀀스를 1부터 다시 시작합니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            cursor.execute("TRUNCATE TABLE LOAD_TEST")
            cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
            cursor.execute("CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE ORDER")
            connection.commit()
            logger.info("Table LOAD_TEST truncated and sequence LOAD_TEST_SEQ reset to 1")
        except Exception as e:
            logger.error(f"Failed to truncate Oracle table: {e}")
            raise
        finally:
            cursor.close()


# ============================================================================
# PostgreSQL JDBC 어댑터
# ============================================================================
class PostgreSQLJDBCAdapter(DatabaseAdapter):
    """PostgreSQL JDBC 어댑터

    PostgreSQL 데이터베이스에 JDBC를 통해 연결하고 SQL을 실행합니다.
    BIGSERIAL 컬럼과 RETURNING 절을 사용하여 자동 증가 ID를 관리합니다.
    """

    def __init__(self, jre_dir: str = './jre'):
        self.pool: Optional[JDBCConnectionPool] = None
        jar_file = find_jdbc_jar('postgresql', jre_dir)
        if not jar_file:
            raise RuntimeError("PostgreSQL JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        jdbc_url = JDBC_DRIVERS['postgresql'].url_template.format(
            host=config.host, port=config.port or 5432, database=config.database
        )
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url, driver_class=JDBC_DRIVERS['postgresql'].driver_class,
            jar_file=self.jar_file, user=config.user, password=config.password,
            min_size=config.min_pool_size, max_size=config.max_pool_size,
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds
        )
        return self.pool

    def get_connection(self):
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        return self.pool.acquire()

    def release_connection(self, connection, is_error: bool = False):
        if connection and self.pool:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except:
                pass

    def discard_connection(self, connection):
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP) RETURNING id
        """, [thread_id, f'TEST_{thread_id}', random_data])
        result = cursor.fetchone()
        return int(result[0])

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행

        지정된 크기만큼 INSERT 문을 반복 실행하여 대량 삽입을 수행합니다.

        Args:
            cursor: 데이터베이스 커서
            thread_id: 워커 스레드 식별자
            batch_size: 배치 크기 (삽입할 레코드 수)

        Returns:
            삽입된 레코드 수 (batch_size)
        """
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO load_test (thread_id, value_col, random_data, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, [thread_id, f'TEST_{thread_id}', random_data])
        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """단일 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            record_id: 조회할 레코드 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            max_id: 조회 가능한 최대 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        if max_id <= 0:
            return None
        random_id = random.randint(1, max_id)
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [random_id])
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 업데이트할 레코드 ID

        Returns:
            업데이트 성공 시 True, 실패 시 False
        """
        cursor.execute("UPDATE load_test SET value_col = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                       [f'UPDATED_{record_id}', record_id])
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 삭제할 레코드 ID

        Returns:
            삭제 성공 시 True, 실패 시 False
        """
        cursor.execute("DELETE FROM load_test WHERE id = ?", [record_id])
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회

        Args:
            cursor: 데이터베이스 커서

        Returns:
            최대 ID 값, 레코드가 없으면 0
        """
        cursor.execute("SELECT COALESCE(MAX(id), 0) FROM load_test")
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성

        Args:
            cursor: 데이터베이스 커서 (미사용)
            max_id: 최대 ID 범위

        Returns:
            1과 max_id 사이의 랜덤 정수
        """
        return random.randint(1, max_id) if max_id > 0 else 0

    def commit(self, connection):
        """트랜잭션 커밋

        Args:
            connection: 데이터베이스 커넥션
        """
        connection.commit()

    def rollback(self, connection):
        """트랜잭션 롤백

        Args:
            connection: 데이터베이스 커넥션
        """
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        return """
-- PostgreSQL DDL
CREATE TABLE load_test (
    id BIGSERIAL PRIMARY KEY, thread_id VARCHAR(50) NOT NULL,
    value_col VARCHAR(200), random_data VARCHAR(1000),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY HASH (id);
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'load_test'"
            )
            result = cursor.fetchone()
            if result and result[0] > 0:
                logger.info("PostgreSQL schema already exists - reusing existing schema")
                logger.info("  (DROP TABLE load_test CASCADE to recreate, or use --truncate to clear data only)")
                return

            cursor.execute("""
                CREATE TABLE load_test (
                    id BIGSERIAL PRIMARY KEY, thread_id VARCHAR(50) NOT NULL,
                    value_col VARCHAR(200), random_data VARCHAR(1000),
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) PARTITION BY HASH (id)
            """)
            for i in range(16):
                cursor.execute(
                    f"CREATE TABLE load_test_p{i:02d} PARTITION OF load_test "
                    f"FOR VALUES WITH (MODULUS 16, REMAINDER {i})"
                )
            cursor.execute("CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at)")
            connection.commit()
            logger.info("PostgreSQL schema created successfully")
        except Exception as e:
            logger.error(f"Failed to setup PostgreSQL schema: {e}")
            raise
        finally:
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 시퀀스 초기화

        load_test 테이블의 모든 데이터를 삭제하고
        IDENTITY 시퀀스를 1부터 다시 시작합니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            cursor.execute("TRUNCATE TABLE load_test RESTART IDENTITY")
            connection.commit()
            logger.info("Table load_test truncated and sequence reset to 1")
        except Exception as e:
            logger.error(f"Failed to truncate PostgreSQL table: {e}")
            raise
        finally:
            cursor.close()


# ============================================================================
# MySQL JDBC 어댑터
# ============================================================================
# MySQL 커넥션 풀 크기 제한 상수
# 이 제한은 MySQL Connector/J의 기본 설정 및 MySQL 서버의 max_connections 설정과 관련됩니다.
# MySQL 서버의 기본 max_connections는 151이지만, 단일 애플리케이션에서 너무 많은
# 커넥션을 사용하면 다른 클라이언트의 연결이 거부될 수 있습니다.
# 또한 MySQL Connector/J의 경우 많은 수의 동시 커넥션에서 성능 저하가 발생할 수 있습니다.
# 필요시 이 값을 조정할 수 있지만, MySQL 서버의 max_connections 설정도 함께 조정해야 합니다.
# MySQL 커넥션 풀 최대 크기 제한 상수
MYSQL_MAX_POOL_SIZE = 32


class MySQLJDBCAdapter(DatabaseAdapter):
    """MySQL JDBC 어댑터

    Note:
        MySQL의 커넥션 풀 크기는 MYSQL_MAX_POOL_SIZE(기본 32)로 제한됩니다.
        이는 다음과 같은 이유로 설정되었습니다:
        1. MySQL 서버의 기본 max_connections (151)와의 균형
        2. MySQL Connector/J의 대규모 동시 연결 시 성능 특성
        3. 단일 애플리케이션이 서버 리소스를 독점하는 것을 방지

        더 많은 커넥션이 필요한 경우:
        - MySQL 서버의 max_connections 설정을 증가시키세요
        - MYSQL_MAX_POOL_SIZE 상수를 조정하세요
    """

    def __init__(self, jre_dir: str = './jre'):
        # 커넥션 풀 초기화 (None으로 시작)
        self.pool: Optional[JDBCConnectionPool] = None
        # MySQL JDBC 드라이버 JAR 파일 검색
        jar_file = find_jdbc_jar('mysql', jre_dir)
        if not jar_file:
            # JDBC 드라이버를 찾지 못한 경우 예외 발생
            raise RuntimeError("MySQL JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        # MySQL JDBC 연결 URL 생성 (기본 포트: 3306)
        jdbc_url = JDBC_DRIVERS['mysql'].url_template.format(
            host=config.host, port=config.port or 3306, database=config.database
        )

        # MySQL 커넥션 풀 크기 제한 적용 (최대 크기 초과 방지)
        effective_min = min(config.min_pool_size, MYSQL_MAX_POOL_SIZE)
        effective_max = min(config.max_pool_size, MYSQL_MAX_POOL_SIZE)

        # 요청된 풀 크기가 제한을 초과하는 경우 경고 로그 출력
        if config.min_pool_size > MYSQL_MAX_POOL_SIZE or config.max_pool_size > MYSQL_MAX_POOL_SIZE:
            logger.warning(
                f"[MySQL] Pool size limited to {MYSQL_MAX_POOL_SIZE} "
                f"(requested: min={config.min_pool_size}, max={config.max_pool_size}). "
                f"See MYSQL_MAX_POOL_SIZE constant for details."
            )

        # JDBC 커넥션 풀 생성 및 설정 적용
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url, driver_class=JDBC_DRIVERS['mysql'].driver_class,
            jar_file=self.jar_file, user=config.user, password=config.password,
            min_size=effective_min, max_size=effective_max,
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds
        )
        return self.pool

    def get_connection(self):
        # 풀이 초기화되지 않은 경우 예외 발생
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        # 풀에서 커넥션 획득
        return self.pool.acquire()

    def release_connection(self, connection, is_error: bool = False):
        # 커넥션과 풀이 유효한 경우에만 처리
        if connection and self.pool:
            try:
                if is_error:
                    # 에러 발생 시 트랜잭션 롤백
                    connection.rollback()
                # 커넥션을 풀에 반환
                self.pool.release(connection)
            except:
                # 반환 중 예외 발생 시 무시
                pass

    def discard_connection(self, connection):
        # 커넥션과 풀이 유효한 경우 커넥션 폐기
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        # 풀이 존재하는 경우 모든 커넥션 종료
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        # 풀이 존재하면 통계 반환, 없으면 빈 딕셔너리 반환
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        # INSERT 쿼리 실행 (NOW()로 현재 시간 삽입)
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, NOW())
        """, [thread_id, f'TEST_{thread_id}', random_data])
        # 방금 삽입된 행의 AUTO_INCREMENT 값 조회
        cursor.execute("SELECT LAST_INSERT_ID()")
        result = cursor.fetchone()
        # 삽입된 ID 값 반환
        return int(result[0])

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행

        지정된 크기만큼 INSERT 문을 반복 실행하여 대량 삽입을 수행합니다.

        Args:
            cursor: 데이터베이스 커서
            thread_id: 워커 스레드 식별자
            batch_size: 배치 크기 (삽입할 레코드 수)

        Returns:
            삽입된 레코드 수 (batch_size)
        """
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO load_test (thread_id, value_col, random_data, created_at)
                VALUES (?, ?, ?, NOW())
            """, [thread_id, f'TEST_{thread_id}', random_data])
        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """단일 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            record_id: 조회할 레코드 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            max_id: 조회 가능한 최대 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        if max_id <= 0:
            return None
        random_id = random.randint(1, max_id)
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [random_id])
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 업데이트할 레코드 ID

        Returns:
            업데이트 성공 시 True, 실패 시 False
        """
        cursor.execute("UPDATE load_test SET value_col = ? WHERE id = ?", [f'UPDATED_{record_id}', record_id])
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 삭제할 레코드 ID

        Returns:
            삭제 성공 시 True, 실패 시 False
        """
        cursor.execute("DELETE FROM load_test WHERE id = ?", [record_id])
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회

        Args:
            cursor: 데이터베이스 커서

        Returns:
            최대 ID 값, 레코드가 없으면 0
        """
        cursor.execute("SELECT IFNULL(MAX(id), 0) FROM load_test")
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성

        Args:
            cursor: 데이터베이스 커서 (미사용)
            max_id: 최대 ID 범위

        Returns:
            1과 max_id 사이의 랜덤 정수
        """
        return random.randint(1, max_id) if max_id > 0 else 0

    def commit(self, connection):
        """트랜잭션 커밋

        Args:
            connection: 데이터베이스 커넥션
        """
        connection.commit()

    def rollback(self, connection):
        """트랜잭션 롤백

        Args:
            connection: 데이터베이스 커넥션
        """
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        """테이블 생성 DDL 반환

        Returns:
            MySQL용 테이블 생성 SQL 문자열
        """
        return """
-- MySQL DDL
CREATE TABLE load_test (
    id BIGINT NOT NULL AUTO_INCREMENT, thread_id VARCHAR(50) NOT NULL,
    value_col VARCHAR(200), random_data VARCHAR(1000),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB PARTITION BY HASH(id) PARTITIONS 16;
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'load_test'"
            )
            result = cursor.fetchone()
            if result and result[0] > 0:
                logger.info("MySQL schema already exists - reusing existing schema")
                logger.info("  (DROP TABLE load_test to recreate, or use --truncate to clear data only)")
                return

            cursor.execute("""
                CREATE TABLE load_test (
                    id BIGINT NOT NULL AUTO_INCREMENT, thread_id VARCHAR(50) NOT NULL,
                    value_col VARCHAR(200), random_data VARCHAR(1000),
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB PARTITION BY HASH(id) PARTITIONS 16
            """)
            cursor.execute("CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at)")
            connection.commit()
            logger.info("MySQL schema created successfully")
        except Exception as e:
            logger.error(f"Failed to setup MySQL schema: {e}")
            raise
        finally:
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 AUTO_INCREMENT 초기화

        load_test 테이블의 모든 데이터를 삭제하고
        AUTO_INCREMENT를 1부터 다시 시작합니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            cursor.execute("TRUNCATE TABLE load_test")
            connection.commit()
            logger.info("Table load_test truncated and AUTO_INCREMENT reset to 1")
        except Exception as e:
            logger.error(f"Failed to truncate MySQL table: {e}")
            raise
        finally:
            cursor.close()


# ============================================================================
# SQL Server JDBC 어댑터
# ============================================================================
class SQLServerJDBCAdapter(DatabaseAdapter):
    """SQL Server JDBC 어댑터

    Microsoft SQL Server 데이터베이스에 JDBC를 통해 연결하고 SQL을 실행합니다.
    IDENTITY 컬럼과 SCOPE_IDENTITY() 함수를 사용하여 자동 증가 ID를 관리합니다.
    """

    def __init__(self, jre_dir: str = './jre'):
        # 커넥션 풀 초기화 (None으로 시작)
        self.pool: Optional[JDBCConnectionPool] = None
        # SQL Server JDBC 드라이버 JAR 파일 검색
        jar_file = find_jdbc_jar('sqlserver', jre_dir)
        if not jar_file:
            # JDBC 드라이버를 찾지 못한 경우 예외 발생
            raise RuntimeError("SQL Server JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        # SQL Server JDBC 연결 URL 생성 (기본 포트: 1433)
        jdbc_url = JDBC_DRIVERS['sqlserver'].url_template.format(
            host=config.host, port=config.port or 1433, database=config.database
        )
        # JDBC 커넥션 풀 생성 및 설정 적용
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url, driver_class=JDBC_DRIVERS['sqlserver'].driver_class,
            jar_file=self.jar_file, user=config.user, password=config.password,
            min_size=config.min_pool_size, max_size=config.max_pool_size,
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds
        )
        return self.pool

    def get_connection(self):
        # 풀이 초기화되지 않은 경우 예외 발생
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        # 풀에서 커넥션 획득
        return self.pool.acquire()

    def release_connection(self, connection, is_error: bool = False):
        # 커넥션과 풀이 유효한 경우에만 처리
        if connection and self.pool:
            try:
                if is_error:
                    # 에러 발생 시 트랜잭션 롤백
                    connection.rollback()
                # 커넥션을 풀에 반환
                self.pool.release(connection)
            except:
                # 반환 중 예외 발생 시 무시
                pass

    def discard_connection(self, connection):
        # 커넥션과 풀이 유효한 경우 커넥션 폐기
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        # 풀이 존재하는 경우 모든 커넥션 종료
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        # 풀이 존재하면 통계 반환, 없으면 빈 딕셔너리 반환
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        # INSERT 쿼리 실행 (GETDATE()로 현재 시간 삽입)
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, GETDATE())
        """, [thread_id, f'TEST_{thread_id}', random_data])
        # 방금 삽입된 행의 IDENTITY 값 조회
        cursor.execute("SELECT SCOPE_IDENTITY()")
        result = cursor.fetchone()
        # 삽입된 ID 값 반환
        return int(result[0])

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행

        지정된 크기만큼 INSERT 문을 반복 실행하여 대량 삽입을 수행합니다.

        Args:
            cursor: 데이터베이스 커서
            thread_id: 워커 스레드 식별자
            batch_size: 배치 크기 (삽입할 레코드 수)

        Returns:
            삽입된 레코드 수 (batch_size)
        """
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO load_test (thread_id, value_col, random_data, created_at)
                VALUES (?, ?, ?, GETDATE())
            """, [thread_id, f'TEST_{thread_id}', random_data])
        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """단일 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            record_id: 조회할 레코드 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            max_id: 조회 가능한 최대 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        if max_id <= 0:
            return None
        random_id = random.randint(1, max_id)
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [random_id])
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 업데이트할 레코드 ID

        Returns:
            업데이트 성공 시 True, 실패 시 False
        """
        cursor.execute("UPDATE load_test SET value_col = ?, updated_at = GETDATE() WHERE id = ?",
                       [f'UPDATED_{record_id}', record_id])
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 삭제할 레코드 ID

        Returns:
            삭제 성공 시 True, 실패 시 False
        """
        cursor.execute("DELETE FROM load_test WHERE id = ?", [record_id])
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회

        Args:
            cursor: 데이터베이스 커서

        Returns:
            최대 ID 값, 레코드가 없으면 0
        """
        cursor.execute("SELECT ISNULL(MAX(id), 0) FROM load_test")
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성

        Args:
            cursor: 데이터베이스 커서 (미사용)
            max_id: 최대 ID 범위

        Returns:
            1과 max_id 사이의 랜덤 정수
        """
        return random.randint(1, max_id) if max_id > 0 else 0

    def commit(self, connection):
        """트랜잭션 커밋

        Args:
            connection: 데이터베이스 커넥션
        """
        connection.commit()

    def rollback(self, connection):
        """트랜잭션 롤백

        Args:
            connection: 데이터베이스 커넥션
        """
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        """테이블 생성 DDL 반환

        Returns:
            SQL Server용 테이블 생성 SQL 문자열
        """
        return """
-- SQL Server DDL
CREATE TABLE load_test (
    id BIGINT IDENTITY(1,1) NOT NULL, thread_id NVARCHAR(50) NOT NULL,
    value_col NVARCHAR(200), random_data NVARCHAR(1000),
    status NVARCHAR(20) DEFAULT 'ACTIVE',
    created_at DATETIME2 DEFAULT GETDATE(), updated_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_load_test PRIMARY KEY CLUSTERED (id)
);
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'load_test'"
            )
            result = cursor.fetchone()
            if result and result[0] > 0:
                logger.info("SQL Server schema already exists - reusing existing schema")
                logger.info("  (DROP TABLE load_test to recreate, or use --truncate to clear data only)")
                return

            cursor.execute("""
                CREATE TABLE load_test (
                    id BIGINT IDENTITY(1,1) NOT NULL, thread_id NVARCHAR(50) NOT NULL,
                    value_col NVARCHAR(200), random_data NVARCHAR(1000),
                    status NVARCHAR(20) DEFAULT 'ACTIVE',
                    created_at DATETIME2 DEFAULT GETDATE(), updated_at DATETIME2 DEFAULT GETDATE(),
                    CONSTRAINT PK_load_test PRIMARY KEY CLUSTERED (id)
                )
            """)
            cursor.execute("CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at)")
            connection.commit()
            logger.info("SQL Server schema created successfully")
        except Exception as e:
            logger.error(f"Failed to setup SQL Server schema: {e}")
            raise
        finally:
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 IDENTITY 초기화

        load_test 테이블의 모든 데이터를 삭제하고
        IDENTITY를 1부터 다시 시작합니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            cursor.execute("TRUNCATE TABLE load_test")
            connection.commit()
            logger.info("Table load_test truncated and IDENTITY reset to 1")
        except Exception as e:
            logger.error(f"Failed to truncate SQL Server table: {e}")
            raise
        finally:
            cursor.close()


# ============================================================================
# Tibero JDBC 어댑터
# ============================================================================
class TiberoJDBCAdapter(DatabaseAdapter):
    """Tibero JDBC 어댑터

    Tibero 데이터베이스에 JDBC를 통해 연결하고 SQL을 실행합니다.
    Oracle과 호환되는 시퀀스(LOAD_TEST_SEQ)와 SYSTIMESTAMP를 사용합니다.
    """

    def __init__(self, jre_dir: str = './jre'):
        # 커넥션 풀 초기화 (None으로 시작)
        self.pool: Optional[JDBCConnectionPool] = None
        # Tibero JDBC 드라이버 JAR 파일 검색
        jar_file = find_jdbc_jar('tibero', jre_dir)
        if not jar_file:
            # JDBC 드라이버를 찾지 못한 경우 예외 발생
            raise RuntimeError("Tibero JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        # Tibero JDBC 연결 URL 생성 (기본 포트: 8629)
        jdbc_url = JDBC_DRIVERS['tibero'].url_template.format(
            host=config.host, port=config.port or 8629, sid=config.sid or config.database
        )
        # JDBC 커넥션 풀 생성 및 설정 적용
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url, driver_class=JDBC_DRIVERS['tibero'].driver_class,
            jar_file=self.jar_file, user=config.user, password=config.password,
            min_size=config.min_pool_size, max_size=config.max_pool_size,
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds
        )
        return self.pool

    def get_connection(self):
        # 풀이 초기화되지 않은 경우 예외 발생
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        # 풀에서 커넥션 획득
        return self.pool.acquire()

    def release_connection(self, connection, is_error: bool = False):
        # 커넥션과 풀이 유효한 경우에만 처리
        if connection and self.pool:
            try:
                if is_error:
                    # 에러 발생 시 트랜잭션 롤백
                    connection.rollback()
                # 커넥션을 풀에 반환
                self.pool.release(connection)
            except:
                # 반환 중 예외 발생 시 무시
                pass

    def discard_connection(self, connection):
        # 커넥션과 풀이 유효한 경우 커넥션 폐기
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        # 풀이 존재하는 경우 모든 커넥션 종료
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        # 풀이 존재하면 통계 반환, 없으면 빈 딕셔너리 반환
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        # INSERT 쿼리 실행 (시퀀스로 ID 생성, SYSTIMESTAMP로 현재 시간 삽입)
        cursor.execute("""
            INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
            VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, SYSTIMESTAMP)
        """, [thread_id, f'TEST_{thread_id}', random_data])
        # 방금 삽입된 시퀀스의 현재 값 조회
        cursor.execute("SELECT LOAD_TEST_SEQ.CURRVAL FROM DUAL")
        result = cursor.fetchone()
        # 삽입된 ID 값 반환
        return int(result[0])

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행

        지정된 크기만큼 INSERT 문을 반복 실행하여 대량 삽입을 수행합니다.

        Args:
            cursor: 데이터베이스 커서
            thread_id: 워커 스레드 식별자
            batch_size: 배치 크기 (삽입할 레코드 수)

        Returns:
            삽입된 레코드 수 (batch_size)
        """
        # 500자 랜덤 문자열 생성 (배치 전체에서 동일하게 사용)
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        # 지정된 배치 크기만큼 반복 INSERT
        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
                VALUES (LOAD_TEST_SEQ.NEXTVAL, ?, ?, ?, SYSTIMESTAMP)
            """, [thread_id, f'TEST_{thread_id}', random_data])
        # 삽입된 레코드 수 반환
        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """단일 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            record_id: 조회할 레코드 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        # 지정된 ID로 레코드 조회
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [record_id])
        # 조회 결과 반환 (없으면 None)
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            max_id: 조회 가능한 최대 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        # 최대 ID가 0 이하인 경우 조회 불가
        if max_id <= 0:
            return None
        # 1부터 max_id 사이의 랜덤 ID 생성
        random_id = random.randint(1, max_id)
        # 랜덤 ID로 레코드 조회
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [random_id])
        # 조회 결과 반환 (없으면 None)
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 업데이트할 레코드 ID

        Returns:
            업데이트 성공 시 True, 실패 시 False
        """
        # VALUE_COL과 UPDATED_AT 컬럼 업데이트
        cursor.execute("UPDATE LOAD_TEST SET VALUE_COL = ?, UPDATED_AT = SYSTIMESTAMP WHERE ID = ?",
                       [f'UPDATED_{record_id}', record_id])
        # 영향받은 행이 있으면 True 반환
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 삭제할 레코드 ID

        Returns:
            삭제 성공 시 True, 실패 시 False
        """
        # 지정된 ID의 레코드 삭제
        cursor.execute("DELETE FROM LOAD_TEST WHERE ID = ?", [record_id])
        # 영향받은 행이 있으면 True 반환
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회

        Args:
            cursor: 데이터베이스 커서

        Returns:
            최대 ID 값, 레코드가 없으면 0
        """
        # 테이블에서 최대 ID 조회 (없으면 0 반환)
        cursor.execute("SELECT NVL(MAX(ID), 0) FROM LOAD_TEST")
        result = cursor.fetchone()
        # 결과가 있으면 정수로 변환하여 반환, 없으면 0
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성

        Args:
            cursor: 데이터베이스 커서 (미사용)
            max_id: 최대 ID 범위

        Returns:
            1과 max_id 사이의 랜덤 정수
        """
        return random.randint(1, max_id) if max_id > 0 else 0

    def commit(self, connection):
        """트랜잭션 커밋

        Args:
            connection: 데이터베이스 커넥션
        """
        connection.commit()

    def rollback(self, connection):
        """트랜잭션 롤백

        Args:
            connection: 데이터베이스 커넥션
        """
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        """테이블 생성 DDL 반환

        Returns:
            Tibero용 테이블 생성 SQL 문자열
        """
        return """
-- Tibero DDL
CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE ORDER;
CREATE TABLE LOAD_TEST (
    ID NUMBER(19) NOT NULL, THREAD_ID VARCHAR2(50) NOT NULL,
    VALUE_COL VARCHAR2(200), RANDOM_DATA VARCHAR2(1000),
    STATUS VARCHAR2(20) DEFAULT 'ACTIVE',
    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP, UPDATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
) PARTITION BY HASH (ID) PARTITIONS 16 ENABLE ROW MOVEMENT;
ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID);
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            table_exists = False
            seq_exists = False

            cursor.execute("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'LOAD_TEST'")
            result = cursor.fetchone()
            if result and result[0] > 0:
                table_exists = True

            cursor.execute("SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'LOAD_TEST_SEQ'")
            result = cursor.fetchone()
            if result and result[0] > 0:
                seq_exists = True

            if table_exists and seq_exists:
                logger.info("Tibero schema already exists - reusing existing schema")
                logger.info("  (DROP objects manually to recreate, or use --truncate to clear data only)")
                return

            if seq_exists:
                try:
                    cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
                except:
                    pass
            if table_exists:
                try:
                    cursor.execute("DROP TABLE LOAD_TEST PURGE")
                except:
                    pass

            cursor.execute("CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE ORDER")
            cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID NUMBER(19) NOT NULL, THREAD_ID VARCHAR2(50) NOT NULL,
                    VALUE_COL VARCHAR2(200), RANDOM_DATA VARCHAR2(1000),
                    STATUS VARCHAR2(20) DEFAULT 'ACTIVE',
                    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                    UPDATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
                ) PARTITION BY HASH (ID) PARTITIONS 16 ENABLE ROW MOVEMENT
            """)
            cursor.execute("ALTER TABLE LOAD_TEST ADD CONSTRAINT PK_LOAD_TEST PRIMARY KEY (ID)")
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT) LOCAL")
            connection.commit()
            logger.info("Tibero schema created successfully")
        except Exception as e:
            # 스키마 생성 실패 시 에러 로그 출력 및 예외 재발생
            logger.error(f"Failed to setup Tibero schema: {e}")
            raise
        finally:
            # 커서 리소스 정리
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 시퀀스 초기화

        LOAD_TEST 테이블의 모든 데이터를 삭제하고
        LOAD_TEST_SEQ 시퀀스를 1부터 다시 시작합니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            # 테이블의 모든 데이터 삭제 (빠른 삭제)
            cursor.execute("TRUNCATE TABLE LOAD_TEST")
            # 기존 시퀀스 삭제
            cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
            # 시퀀스를 1부터 다시 생성 (캐시 1000, 순환 없음, 순서 보장)
            cursor.execute("CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE ORDER")
            # 변경사항 커밋
            connection.commit()
            logger.info("Table LOAD_TEST truncated and sequence LOAD_TEST_SEQ reset to 1")
        except Exception as e:
            # 테이블 초기화 실패 시 에러 로그 출력 및 예외 재발생
            logger.error(f"Failed to truncate Tibero table: {e}")
            raise
        finally:
            # 커서 리소스 정리
            cursor.close()


# ============================================================================
# SingleStore JDBC 어댑터
# ============================================================================
# SingleStore 커넥션 풀 크기 제한 상수
# 이 제한은 SingleStore의 기본 설정과 관련됩니다.
# 필요시 이 값을 조정할 수 있습니다.
SINGLESTORE_MAX_POOL_SIZE = 64


class SingleStoreJDBCAdapter(DatabaseAdapter):
    """SingleStore JDBC 어댑터

    SingleStore 데이터베이스에 JDBC를 통해 연결하고 SQL을 실행합니다.
    SingleStore는 MySQL 프로토콜과 호환되므로 MySQL과 유사한 SQL을 사용합니다.
    """

    def __init__(self, jre_dir: str = './jre'):
        # 커넥션 풀 초기화 (None으로 시작)
        self.pool: Optional[JDBCConnectionPool] = None
        # SingleStore JDBC 드라이버 JAR 파일 검색
        jar_file = find_jdbc_jar('singlestore', jre_dir)
        if not jar_file:
            # JDBC 드라이버를 찾지 못한 경우 예외 발생
            raise RuntimeError("SingleStore JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        # SingleStore JDBC 연결 URL 생성 (기본 포트: 3306)
        jdbc_url = JDBC_DRIVERS['singlestore'].url_template.format(
            host=config.host, port=config.port or 3306, database=config.database
        )

        # SingleStore 커넥션 풀 크기 제한 적용 (최대 크기 초과 방지)
        effective_min = min(config.min_pool_size, SINGLESTORE_MAX_POOL_SIZE)
        effective_max = min(config.max_pool_size, SINGLESTORE_MAX_POOL_SIZE)

        # 요청된 풀 크기가 제한을 초과하는 경우 경고 로그 출력
        if config.min_pool_size > SINGLESTORE_MAX_POOL_SIZE or config.max_pool_size > SINGLESTORE_MAX_POOL_SIZE:
            logger.warning(
                f"[SingleStore] Pool size limited to {SINGLESTORE_MAX_POOL_SIZE} "
                f"(requested: min={config.min_pool_size}, max={config.max_pool_size}). "
                f"See SINGLESTORE_MAX_POOL_SIZE constant for details."
            )

        # JDBC 커넥션 풀 생성 및 설정 적용
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url, driver_class=JDBC_DRIVERS['singlestore'].driver_class,
            jar_file=self.jar_file, user=config.user, password=config.password,
            min_size=effective_min, max_size=effective_max,
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds
        )
        return self.pool

    def get_connection(self):
        # 풀이 초기화되지 않은 경우 예외 발생
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        # 풀에서 커넥션 획득
        return self.pool.acquire()

    def release_connection(self, connection, is_error: bool = False):
        # 커넥션과 풀이 유효한 경우에만 처리
        if connection and self.pool:
            try:
                if is_error:
                    # 에러 발생 시 트랜잭션 롤백
                    connection.rollback()
                # 커넥션을 풀에 반환
                self.pool.release(connection)
            except:
                # 반환 중 예외 발생 시 무시
                pass

    def discard_connection(self, connection):
        # 커넥션과 풀이 유효한 경우 커넥션 폐기
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        # 풀이 존재하는 경우 모든 커넥션 종료
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        # 풀이 존재하면 통계 반환, 없으면 빈 딕셔너리 반환
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        # INSERT 쿼리 실행 (NOW()로 현재 시간 삽입)
        cursor.execute("""
            INSERT INTO load_test (thread_id, value_col, random_data, created_at)
            VALUES (?, ?, ?, NOW())
        """, [thread_id, f'TEST_{thread_id}', random_data])
        # 방금 삽입된 행의 AUTO_INCREMENT 값 조회
        cursor.execute("SELECT LAST_INSERT_ID()")
        result = cursor.fetchone()
        # 삽입된 ID 값 반환
        return int(result[0])

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행

        지정된 크기만큼 INSERT 문을 반복 실행하여 대량 삽입을 수행합니다.

        Args:
            cursor: 데이터베이스 커서
            thread_id: 워커 스레드 식별자
            batch_size: 배치 크기 (삽입할 레코드 수)

        Returns:
            삽입된 레코드 수 (batch_size)
        """
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO load_test (thread_id, value_col, random_data, created_at)
                VALUES (?, ?, ?, NOW())
            """, [thread_id, f'TEST_{thread_id}', random_data])
        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """단일 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            record_id: 조회할 레코드 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [record_id])
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            max_id: 조회 가능한 최대 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        if max_id <= 0:
            return None
        random_id = random.randint(1, max_id)
        cursor.execute("SELECT id, thread_id, value_col FROM load_test WHERE id = ?", [random_id])
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 업데이트할 레코드 ID

        Returns:
            업데이트 성공 시 True, 실패 시 False
        """
        cursor.execute("UPDATE load_test SET value_col = ? WHERE id = ?", [f'UPDATED_{record_id}', record_id])
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 삭제할 레코드 ID

        Returns:
            삭제 성공 시 True, 실패 시 False
        """
        cursor.execute("DELETE FROM load_test WHERE id = ?", [record_id])
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회

        Args:
            cursor: 데이터베이스 커서

        Returns:
            최대 ID 값, 레코드가 없으면 0
        """
        cursor.execute("SELECT IFNULL(MAX(id), 0) FROM load_test")
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성

        Args:
            cursor: 데이터베이스 커서 (미사용)
            max_id: 최대 ID 범위

        Returns:
            1과 max_id 사이의 랜덤 정수
        """
        return random.randint(1, max_id) if max_id > 0 else 0

    def commit(self, connection):
        """트랜잭션 커밋

        Args:
            connection: 데이터베이스 커넥션
        """
        connection.commit()

    def rollback(self, connection):
        """트랜잭션 롤백

        Args:
            connection: 데이터베이스 커넥션
        """
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        """테이블 생성 DDL 반환

        Returns:
            SingleStore용 테이블 생성 SQL 문자열
        """
        return """
-- SingleStore DDL
CREATE TABLE load_test (
    id BIGINT NOT NULL AUTO_INCREMENT, thread_id VARCHAR(50) NOT NULL,
    value_col VARCHAR(200), random_data VARCHAR(1000),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=ColumnStore;
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'load_test'"
            )
            result = cursor.fetchone()
            if result and result[0] > 0:
                logger.info("SingleStore schema already exists - reusing existing schema")
                logger.info("  (DROP TABLE load_test to recreate, or use --truncate to clear data only)")
                return

            cursor.execute("""
                CREATE TABLE load_test (
                    id BIGINT NOT NULL AUTO_INCREMENT, thread_id VARCHAR(50) NOT NULL,
                    value_col VARCHAR(200), random_data VARCHAR(1000),
                    status VARCHAR(20) DEFAULT 'ACTIVE',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id)
                ) ENGINE=ColumnStore
            """)
            cursor.execute("CREATE INDEX idx_load_test_thread ON load_test(thread_id, created_at)")
            connection.commit()
            logger.info("SingleStore schema created successfully")
        except Exception as e:
            logger.error(f"Failed to setup SingleStore schema: {e}")
            raise
        finally:
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 AUTO_INCREMENT 초기화

        load_test 테이블의 모든 데이터를 삭제하고
        AUTO_INCREMENT를 1부터 다시 시작합니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            cursor.execute("TRUNCATE TABLE load_test")
            connection.commit()
            logger.info("Table load_test truncated and AUTO_INCREMENT reset to 1")
        except Exception as e:
            logger.error(f"Failed to truncate SingleStore table: {e}")
            raise
        finally:
            cursor.close()


# ============================================================================
# DB2 JDBC 어댑터
# ============================================================================
class DB2JDBCAdapter(DatabaseAdapter):
    """IBM DB2 JDBC 어댑터

    IBM DB2 데이터베이스에 JDBC를 통해 연결하고 SQL을 실행합니다.
    """

    def __init__(self, jre_dir: str = './jre'):
        # 커넥션 풀 초기화 (None으로 시작)
        self.pool: Optional[JDBCConnectionPool] = None
        # DB2 JDBC 드라이버 JAR 파일 검색
        jar_file = find_jdbc_jar('db2', jre_dir)
        if not jar_file:
            # JDBC 드라이버를 찾지 못한 경우 예외 발생
            raise RuntimeError("DB2 JDBC driver not found")
        self.jar_file: str = jar_file

    def create_connection_pool(self, config: 'DatabaseConfig'):
        # DB2 JDBC 연결 URL 생성 (기본 포트: 50000)
        jdbc_url = JDBC_DRIVERS['db2'].url_template.format(
            host=config.host, port=config.port or 50000, database=config.database
        )
        # JDBC 커넥션 풀 생성 및 설정 적용
        self.pool = JDBCConnectionPool(
            jdbc_url=jdbc_url, driver_class=JDBC_DRIVERS['db2'].driver_class,
            jar_file=self.jar_file, user=config.user, password=config.password,
            min_size=config.min_pool_size, max_size=config.max_pool_size,
            max_lifetime_seconds=config.max_lifetime_seconds,
            leak_detection_threshold_seconds=config.leak_detection_threshold_seconds,
            idle_check_interval_seconds=config.idle_check_interval_seconds,
            idle_timeout_seconds=config.idle_timeout_seconds,
            keepalive_time_seconds=config.keepalive_time_seconds
        )
        return self.pool

    def get_connection(self):
        # 풀이 초기화되지 않은 경우 예외 발생
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        # 풀에서 커넥션 획득
        return self.pool.acquire()

    def release_connection(self, connection, is_error: bool = False):
        if connection and self.pool:
            try:
                if is_error:
                    connection.rollback()
                self.pool.release(connection)
            except:
                pass

    def discard_connection(self, connection):
        if connection and self.pool:
            self.pool.discard(connection)

    def close_pool(self):
        if self.pool:
            self.pool.close_all()

    def get_pool_stats(self) -> Dict[str, Union[int, str]]:
        return self.pool.get_pool_stats() if self.pool else {}

    def execute_insert(self, cursor, thread_id: str, random_data: str) -> int:
        cursor.execute("""
            INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
            VALUES (NEXT VALUE FOR LOAD_TEST_SEQ, ?, ?, ?, CURRENT TIMESTAMP)
        """, [thread_id, f'TEST_{thread_id}', random_data])

        cursor.execute("SELECT PREVIOUS VALUE FOR LOAD_TEST_SEQ FROM SYSIBM.SYSDUMMY1")
        result = cursor.fetchone()
        return int(result[0]) if result else -1

    def execute_batch_insert(self, cursor, thread_id: str, batch_size: int) -> int:
        """배치 INSERT 실행

        지정된 크기만큼 INSERT 문을 반복 실행하여 대량 삽입을 수행합니다.

        Args:
            cursor: 데이터베이스 커서
            thread_id: 워커 스레드 식별자
            batch_size: 배치 크기 (삽입할 레코드 수)

        Returns:
            삽입된 레코드 수 (batch_size)
        """
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        for _ in range(batch_size):
            cursor.execute("""
                INSERT INTO LOAD_TEST (ID, THREAD_ID, VALUE_COL, RANDOM_DATA, CREATED_AT)
                VALUES (NEXT VALUE FOR LOAD_TEST_SEQ, ?, ?, ?, CURRENT TIMESTAMP)
            """, [thread_id, f'TEST_{thread_id}', random_data])
        return batch_size

    def execute_select(self, cursor, record_id: int) -> Optional[tuple]:
        """단일 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            record_id: 조회할 레코드 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [record_id])
        return cursor.fetchone()

    def execute_random_select(self, cursor, max_id: int) -> Optional[tuple]:
        """랜덤 레코드 조회

        Args:
            cursor: 데이터베이스 커서
            max_id: 조회 가능한 최대 ID

        Returns:
            조회된 레코드 튜플, 없으면 None
        """
        if max_id <= 0:
            return None
        random_id = random.randint(1, max_id)
        cursor.execute("SELECT ID, THREAD_ID, VALUE_COL FROM LOAD_TEST WHERE ID = ?", [random_id])
        return cursor.fetchone()

    def execute_update(self, cursor, record_id: int) -> bool:
        """레코드 UPDATE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 업데이트할 레코드 ID

        Returns:
            업데이트 성공 시 True, 실패 시 False
        """
        cursor.execute(
            "UPDATE LOAD_TEST SET VALUE_COL = ?, UPDATED_AT = CURRENT TIMESTAMP WHERE ID = ?",
            [f'UPDATED_{record_id}', record_id]
        )
        return cursor.rowcount > 0

    def execute_delete(self, cursor, record_id: int) -> bool:
        """레코드 DELETE 실행

        Args:
            cursor: 데이터베이스 커서
            record_id: 삭제할 레코드 ID

        Returns:
            삭제 성공 시 True, 실패 시 False
        """
        cursor.execute("DELETE FROM LOAD_TEST WHERE ID = ?", [record_id])
        return cursor.rowcount > 0

    def get_max_id(self, cursor) -> int:
        """테이블의 최대 ID 조회

        Args:
            cursor: 데이터베이스 커서

        Returns:
            최대 ID 값, 레코드가 없으면 0
        """
        cursor.execute("SELECT COALESCE(MAX(ID), 0) FROM LOAD_TEST")
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    def get_random_id(self, cursor, max_id: int) -> int:
        """랜덤 ID 생성

        Args:
            cursor: 데이터베이스 커서 (미사용)
            max_id: 최대 ID 범위

        Returns:
            1과 max_id 사이의 랜덤 정수
        """
        return random.randint(1, max_id) if max_id > 0 else 0

    def commit(self, connection):
        """트랜잭션 커밋

        Args:
            connection: 데이터베이스 커넥션
        """
        connection.commit()

    def rollback(self, connection):
        """트랜잭션 롤백

        Args:
            connection: 데이터베이스 커넥션
        """
        try:
            connection.rollback()
        except:
            pass

    def get_ddl(self) -> str:
        """테이블 생성 DDL 반환

        Returns:
            DB2용 테이블 생성 SQL 문자열
        """
        return """
-- IBM DB2 DDL
CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NO CYCLE ORDER;

CREATE TABLE LOAD_TEST (
    ID           BIGINT          NOT NULL,
    THREAD_ID    VARCHAR(50)     NOT NULL,
    VALUE_COL    VARCHAR(200),
    RANDOM_DATA  VARCHAR(1000),
    STATUS       VARCHAR(20)     DEFAULT 'ACTIVE',
    CREATED_AT   TIMESTAMP       DEFAULT CURRENT TIMESTAMP,
    UPDATED_AT   TIMESTAMP       DEFAULT CURRENT TIMESTAMP,
    PRIMARY KEY (ID)
);

CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT);
"""

    def setup_schema(self, connection):
        cursor = connection.cursor()
        try:
            table_exists = False
            seq_exists = False

            cursor.execute("SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABNAME = 'LOAD_TEST'")
            result = cursor.fetchone()
            if result and result[0] > 0:
                table_exists = True

            cursor.execute("SELECT COUNT(*) FROM SYSCAT.SEQUENCES WHERE SEQNAME = 'LOAD_TEST_SEQ'")
            result = cursor.fetchone()
            if result and result[0] > 0:
                seq_exists = True

            if table_exists and seq_exists:
                logger.info("DB2 schema already exists - reusing existing schema")
                logger.info("  (DROP objects manually to recreate, or use --truncate to clear data only)")
                return

            if seq_exists:
                try:
                    cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
                except:
                    pass
            if table_exists:
                try:
                    cursor.execute("DROP TABLE LOAD_TEST")
                except:
                    pass

            cursor.execute("CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NO CYCLE ORDER")
            cursor.execute("""
                CREATE TABLE LOAD_TEST (
                    ID BIGINT NOT NULL,
                    THREAD_ID VARCHAR(50) NOT NULL,
                    VALUE_COL VARCHAR(200),
                    RANDOM_DATA VARCHAR(1000),
                    STATUS VARCHAR(20) DEFAULT 'ACTIVE',
                    CREATED_AT TIMESTAMP DEFAULT CURRENT TIMESTAMP,
                    UPDATED_AT TIMESTAMP DEFAULT CURRENT TIMESTAMP,
                    PRIMARY KEY (ID)
                )
            """)
            cursor.execute("CREATE INDEX IDX_LOAD_TEST_THREAD ON LOAD_TEST(THREAD_ID, CREATED_AT)")
            connection.commit()
            logger.info("DB2 schema created successfully")
        except Exception as e:
            logger.error(f"Failed to setup DB2 schema: {e}")
            raise
        finally:
            cursor.close()

    def truncate_table(self, connection):
        """테이블 데이터 삭제 및 시퀀스 초기화

        LOAD_TEST 테이블의 모든 데이터를 삭제하고
        LOAD_TEST_SEQ 시퀀스를 1부터 다시 시작합니다.
        IMMEDIATE 옵션으로 즉시 커밋됩니다.

        Args:
            connection: 데이터베이스 커넥션
        """
        cursor = connection.cursor()
        try:
            cursor.execute("TRUNCATE TABLE LOAD_TEST IMMEDIATE")
            cursor.execute("DROP SEQUENCE LOAD_TEST_SEQ")
            cursor.execute("CREATE SEQUENCE LOAD_TEST_SEQ START WITH 1 INCREMENT BY 1 CACHE 1000 NO CYCLE ORDER")
            connection.commit()
            logger.info("Table LOAD_TEST truncated and sequence LOAD_TEST_SEQ reset to 1")
        except Exception as e:
            logger.error(f"Failed to truncate DB2 table: {e}")
            raise
        finally:
            cursor.close()


# 설정 클래스
# ============================================================================
@dataclass
class DatabaseConfig:
    """데이터베이스 연결 설정

    Attributes:
        db_type: 데이터베이스 타입 (oracle, postgresql, mysql, sqlserver, tibero)
        host: 데이터베이스 호스트
        user: 데이터베이스 사용자
        password: 데이터베이스 비밀번호
        database: 데이터베이스 이름 (PostgreSQL, MySQL, SQL Server)
        sid: Oracle/Tibero SID
        port: 포트 번호
        min_pool_size: 최소 커넥션 풀 크기 (Warm-up 시 생성)
        max_pool_size: 최대 커넥션 풀 크기
        jre_dir: JRE/JDBC 드라이버 디렉터리
        max_lifetime_seconds: 커넥션 최대 수명 (초, 기본 30분)
        leak_detection_threshold_seconds: Leak 감지 임계값 (초, 기본 60초)
        idle_check_interval_seconds: 유휴 커넥션 Health Check 주기 (초, 기본 30초)
    """
    db_type: str
    host: str
    user: str
    password: str
    database: Optional[str] = None
    sid: Optional[str] = None
    service_name: Optional[str] = None
    port: Optional[int] = None
    min_pool_size: int = 100
    max_pool_size: int = 200
    jre_dir: str = './jre'
    connection_timeout_seconds: int = 10  # 기본 10초
    # 커넥션 풀 고급 설정
    max_lifetime_seconds: int = 1800  # 30분
    leak_detection_threshold_seconds: int = 60  # 60초
    idle_check_interval_seconds: int = 30  # 30초
    idle_timeout_seconds: int = 30
    keepalive_time_seconds: int = 30


# ============================================================================
# 부하 테스트 워커 - Enhanced
# ============================================================================
class LoadTestWorker:
    """부하 테스트 워커 - 전체 기능 지원

    각 워커 스레드에서 실행되어 DB 작업을 수행합니다.
    INSERT, SELECT, UPDATE, DELETE 등 다양한 모드를 지원하며,
    커넥션 오류 시 자동 재연결 로직을 포함합니다.

    Attributes:
        ERROR_LOG_INTERVAL_MS: 에러 로그 출력 간격 (밀리초)
        MAX_CONNECTION_RETRIES: 커넥션 획득 최대 재시도 횟수
        MAX_BACKOFF_MS: 최대 백오프 시간 (밀리초)
    """
    ERROR_LOG_INTERVAL_MS = 10000
    MAX_CONNECTION_RETRIES = 3
    MAX_BACKOFF_MS = 5000

    def __init__(self, worker_id: int, db_adapter: DatabaseAdapter, end_time: datetime,
                 mode: str = WorkMode.FULL, max_id_cache: int = 0, batch_size: int = 1,
                 rate_limiter: Optional[RateLimiter] = None, ramp_up_end_time: Optional[datetime] = None):
        """LoadTestWorker 초기화

        Args:
            worker_id: 워커 식별 번호
            db_adapter: 데이터베이스 어댑터
            end_time: 테스트 종료 시간
            mode: 작업 모드 (full, insert-only, select-only 등)
            max_id_cache: 기존 데이터의 최대 ID (캐시)
            batch_size: 배치 INSERT 크기
            rate_limiter: 속도 제한기 (옵션)
            ramp_up_end_time: Ramp-up 종료 시간 (옵션)
        """
        self.worker_id = worker_id
        self.db_adapter = db_adapter
        self.end_time = end_time
        self.mode = mode
        self.max_id_cache = max_id_cache
        self.batch_size = batch_size
        self.rate_limiter = rate_limiter
        self.ramp_up_end_time = ramp_up_end_time
        self.thread_name = f"Worker-{worker_id:04d}"
        self.transaction_count = 0
        self.last_error_log_time = 0
        self.suppressed_error_count = 0
        self.current_backoff_ms = 100

    def generate_random_data(self, length: int = 500) -> str:
        """테스트용 랜덤 문자열 생성

        Args:
            length: 생성할 문자열 길이

        Returns:
            영문자와 숫자로 구성된 랜덤 문자열
        """
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def is_during_ramp_up(self) -> bool:
        """Ramp-up 기간 여부 확인

        Returns:
            Ramp-up 기간 중이면 True, 그렇지 않으면 False
        """
        if self.ramp_up_end_time is None:
            return False
        return datetime.now() < self.ramp_up_end_time

    def _is_connection_valid(self, connection) -> bool:
        """커넥션 유효성 검사

        Args:
            connection: 검사할 커넥션

        Returns:
            유효하면 True, 그렇지 않으면 False
        """
        try:
            if connection is None:
                return False
            jconn = connection.jconn
            if hasattr(jconn, 'isValid'):
                is_valid = jconn.isValid(self.db_adapter.validation_timeout)
                if not is_valid:
                    logger.debug(f"[{self.thread_name}] Connection validation failed (isValid=False)")
                return is_valid
            return True
        except Exception as e:
            logger.debug(f"[{self.thread_name}] Connection validation error: {e}")
            return False

    def _get_valid_connection(self):
        """유효한 커넥션 획득 (재시도 로직 포함)

        커넥션 풀에서 커넥션을 획득하고 유효성을 검사합니다.
        무효한 커넥션은 폐기하고 새로운 커넥션을 시도합니다.

        Returns:
            유효한 커넥션 객체, 모든 재시도 실패 시에도 마지막 시도 결과 반환
        """
        for retry in range(self.MAX_CONNECTION_RETRIES):
            try:
                conn = self.db_adapter.get_connection()
                if conn and self._is_connection_valid(conn):
                    self.current_backoff_ms = 100
                    return conn
                if conn:
                    self.db_adapter.discard_connection(conn)
                    if perf_counter:
                        perf_counter.increment_connection_recreate()
            except Exception:
                pass

            if retry < self.MAX_CONNECTION_RETRIES - 1:
                time.sleep(self.current_backoff_ms / 1000.0)
                self.current_backoff_ms = min(self.current_backoff_ms * 2, self.MAX_BACKOFF_MS)

        return self.db_adapter.get_connection()

    def reset_backoff(self):
        """백오프 시간 초기화 (성공 시 호출)"""
        self.current_backoff_ms = 100

    def log_error(self, operation: str, message: str):
        """에러 로그 기록 (중복 억제)

        동일한 에러가 반복될 때 로그 폭주를 방지하기 위해
        일정 간격으로만 경고 로그를 출력하고, 그 외에는 디버그 레벨로 기록합니다.

        Args:
            operation: 수행 중이던 작업 이름
            message: 에러 메시지
        """
        # if message and (
        #     'Connection is closed' in message or
        #     'connection is closed' in message or
        #     'Already closed' in message or
        #     'No operations allowed after connection closed' in message or
        #     'Connection is not available' in message or
        #     'request timed out' in message
        # ):
        #     logger.debug(f"[{self.thread_name}] {operation} (suppressed): {message}")
        #     return

        now_ms = int(time.time() * 1000)
        if now_ms - self.last_error_log_time > self.ERROR_LOG_INTERVAL_MS:
            if self.suppressed_error_count > 0:
                logger.warning(
                    f"[{self.thread_name}] {operation} error (suppressed {self.suppressed_error_count} similar errors): {message}"
                )
            else:
                logger.warning(f"[{self.thread_name}] {operation} error: {message}")
            self.last_error_log_time = now_ms
            self.suppressed_error_count = 0
        else:
            self.suppressed_error_count += 1
            logger.debug(f"[{self.thread_name}] {operation} error: {message}")

    def execute_insert(self, connection) -> bool:
        """INSERT 작업 실행

        Args:
            connection: 데이터베이스 커넥션

        Returns:
            성공 시 True, 실패 시 False
        """
        cursor = None
        # 작업 시작 시간 기록 (레이턴시 측정용)
        start_time = time.time()
        try:
            # 커서 생성
            cursor = connection.cursor()
            # 워커 스레드 이름을 thread_id로 사용
            thread_id = self.thread_name
            # 테스트용 랜덤 데이터 생성 (500자)
            random_data = self.generate_random_data()

            # 배치 모드 여부에 따른 분기 처리
            if self.batch_size > 1:
                # 배치 INSERT: 지정된 개수만큼 한 번에 삽입
                count = self.db_adapter.execute_batch_insert(cursor, thread_id, self.batch_size)
                # 배치 개수만큼 INSERT 카운터 증가
                if perf_counter:
                    perf_counter.increment_insert(count)
            else:
                # 단일 INSERT: 1건 삽입
                self.db_adapter.execute_insert(cursor, thread_id, random_data)
                # INSERT 카운터 1 증가
                if perf_counter:
                    perf_counter.increment_insert()

            # 트랜잭션 커밋 (데이터 영구 저장)
            self.db_adapter.commit(connection)

            # 레이턴시 계산 (밀리초 단위)
            latency_ms = (time.time() - start_time) * 1000
            # 트랜잭션 완료 기록 (TPS 및 레이턴시 통계용)
            if perf_counter:
                perf_counter.record_transaction(latency_ms)
            # 워커별 트랜잭션 카운트 증가
            self.transaction_count += 1
            return True
        except Exception as e:
            # 에러 발생 시 로그 기록
            self.log_error("Insert", str(e))
            # 에러 카운터 증가
            if perf_counter:
                perf_counter.increment_error()
            # 트랜잭션 롤백 (변경사항 취소)
            self.db_adapter.rollback(connection)
            return False
        finally:
            # 커서 정리 (리소스 해제)
            if cursor:
                try:
                    cursor.close()
                except:
                    # 커서 닫기 실패 시 무시
                    pass

    def execute_select(self, connection, max_id: int) -> bool:
        """SELECT 작업 실행

        Args:
            connection: 데이터베이스 커넥션
            max_id: 조회 가능한 최대 ID

        Returns:
            성공 시 True, 실패 시 False
        """
        cursor = None
        # 작업 시작 시간 기록 (레이턴시 측정용)
        start_time = time.time()
        try:
            # 커서 생성
            cursor = connection.cursor()
            # 1~max_id 범위에서 랜덤 ID로 조회 수행
            self.db_adapter.execute_random_select(cursor, max_id)
            # SELECT 카운터 증가
            if perf_counter:
                perf_counter.increment_select()

            # 레이턴시 계산 (밀리초 단위)
            latency_ms = (time.time() - start_time) * 1000
            # 트랜잭션 완료 기록 (TPS 및 레이턴시 통계용)
            if perf_counter:
                perf_counter.record_transaction(latency_ms)
            # 워커별 트랜잭션 카운트 증가
            self.transaction_count += 1
            return True
        except Exception as e:
            # 에러 발생 시 로그 기록
            self.log_error("Select", str(e))
            # 에러 카운터 증가
            if perf_counter:
                perf_counter.increment_error()
            # SELECT는 읽기 전용이므로 롤백 불필요
            return False
        finally:
            # 커서 정리 (리소스 해제)
            if cursor:
                try:
                    cursor.close()
                except:
                    # 커서 닫기 실패 시 무시
                    pass

    def execute_update(self, connection, max_id: int) -> bool:
        """UPDATE 작업 실행

        Args:
            connection: 데이터베이스 커넥션
            max_id: 업데이트 가능한 최대 ID

        Returns:
            성공 시 True, 실패 시 False
        """
        cursor = None
        # 작업 시작 시간 기록 (레이턴시 측정용)
        start_time = time.time()
        try:
            # 커서 생성
            cursor = connection.cursor()
            # 1~max_id 범위에서 랜덤 ID 선택
            record_id = self.db_adapter.get_random_id(cursor, max_id)
            # 유효한 ID가 없으면 성공으로 처리 (데이터 없음)
            if record_id <= 0:
                return True
            # 선택된 ID의 레코드 업데이트 수행
            self.db_adapter.execute_update(cursor, record_id)
            # 트랜잭션 커밋 (변경사항 영구 저장)
            self.db_adapter.commit(connection)
            # UPDATE 카운터 증가
            if perf_counter:
                perf_counter.increment_update()

            # 레이턴시 계산 (밀리초 단위)
            latency_ms = (time.time() - start_time) * 1000
            # 트랜잭션 완료 기록 (TPS 및 레이턴시 통계용)
            if perf_counter:
                perf_counter.record_transaction(latency_ms)
            # 워커별 트랜잭션 카운트 증가
            self.transaction_count += 1
            return True
        except Exception as e:
            # 에러 발생 시 로그 기록
            self.log_error("Update", str(e))
            # 에러 카운터 증가
            if perf_counter:
                perf_counter.increment_error()
            # 트랜잭션 롤백 (변경사항 취소)
            self.db_adapter.rollback(connection)
            return False
        finally:
            # 커서 정리 (리소스 해제)
            if cursor:
                try:
                    cursor.close()
                except:
                    # 커서 닫기 실패 시 무시
                    pass

    def execute_delete(self, connection, max_id: int) -> bool:
        """DELETE 작업 실행

        Args:
            connection: 데이터베이스 커넥션
            max_id: 삭제 가능한 최대 ID

        Returns:
            성공 시 True, 실패 시 False
        """
        cursor = None
        # 작업 시작 시간 기록 (레이턴시 측정용)
        start_time = time.time()
        try:
            # 커서 생성
            cursor = connection.cursor()
            # 1~max_id 범위에서 랜덤 ID 선택
            record_id = self.db_adapter.get_random_id(cursor, max_id)
            # 유효한 ID가 없으면 성공으로 처리 (데이터 없음)
            if record_id <= 0:
                return True
            # 선택된 ID의 레코드 삭제 수행
            self.db_adapter.execute_delete(cursor, record_id)
            # 트랜잭션 커밋 (삭제 영구 반영)
            self.db_adapter.commit(connection)
            # DELETE 카운터 증가
            if perf_counter:
                perf_counter.increment_delete()

            # 레이턴시 계산 (밀리초 단위)
            latency_ms = (time.time() - start_time) * 1000
            # 트랜잭션 완료 기록 (TPS 및 레이턴시 통계용)
            if perf_counter:
                perf_counter.record_transaction(latency_ms)
            # 워커별 트랜잭션 카운트 증가
            self.transaction_count += 1
            return True
        except Exception as e:
            # 에러 발생 시 로그 기록
            self.log_error("Delete", str(e))
            # 에러 카운터 증가
            if perf_counter:
                perf_counter.increment_error()
            # 트랜잭션 롤백 (삭제 취소)
            self.db_adapter.rollback(connection)
            return False
        finally:
            # 커서 정리 (리소스 해제)
            if cursor:
                try:
                    cursor.close()
                except:
                    # 커서 닫기 실패 시 무시
                    pass

    def execute_mixed(self, connection, max_id: int) -> bool:
        """혼합 모드 작업 실행

        랜덤하게 작업을 선택하여 실행합니다.
        비율: INSERT 60%, SELECT 20%, UPDATE 15%, DELETE 5%

        Args:
            connection: 데이터베이스 커넥션
            max_id: 작업 가능한 최대 ID

        Returns:
            성공 시 True, 실패 시 False
        """
        # 0.0 ~ 1.0 사이의 랜덤 값 생성
        rand = random.random()
        # 60% 확률: INSERT 실행
        if rand < 0.60:
            return self.execute_insert(connection)
        # 20% 확률: SELECT 실행 (누적 80%)
        elif rand < 0.80:
            return self.execute_select(connection, max_id)
        # 15% 확률: UPDATE 실행 (누적 95%)
        elif rand < 0.95:
            return self.execute_update(connection, max_id)
        # 5% 확률: DELETE 실행 (나머지)
        else:
            return self.execute_delete(connection, max_id)

    def execute_full(self, connection) -> bool:
        """전체 트랜잭션 실행

        INSERT -> COMMIT -> SELECT -> VERIFY -> UPDATE -> DELETE
        전체 CRUD 사이클을 하나의 트랜잭션으로 실행합니다.

        Args:
            connection: 데이터베이스 커넥션

        Returns:
            성공 시 True, 실패 시 False
        """
        cursor = None
        # 작업 시작 시간 기록 (전체 CRUD 사이클 레이턴시 측정용)
        start_time = time.time()
        try:
            # 커서 생성
            cursor = connection.cursor()
            # 워커 스레드 이름을 thread_id로 사용
            thread_id = self.thread_name
            # 테스트용 랜덤 데이터 생성 (500자)
            random_data = self.generate_random_data()

            # [1단계] INSERT 실행 - 새 레코드 삽입
            new_id = self.db_adapter.execute_insert(cursor, thread_id, random_data)
            # INSERT 카운터 증가
            if perf_counter:
                perf_counter.increment_insert()
            # INSERT 커밋 (데이터 영구 저장)
            self.db_adapter.commit(connection)

            # [2단계] SELECT 실행 - 방금 삽입한 레코드 조회
            result = self.db_adapter.execute_select(cursor, new_id)
            # SELECT 카운터 증가
            if perf_counter:
                perf_counter.increment_select()

            # [3단계] VERIFY - 조회 결과 검증 (데이터 무결성 확인)
            if result is None or result[0] != new_id:
                # 검증 실패: 삽입한 데이터를 조회할 수 없음
                if perf_counter:
                    perf_counter.increment_verification_failure()
                return False

            # [4단계] UPDATE 실행 - 레코드 수정
            self.db_adapter.execute_update(cursor, new_id)
            # UPDATE 카운터 증가
            if perf_counter:
                perf_counter.increment_update()
            # UPDATE 커밋 (변경사항 영구 저장)
            self.db_adapter.commit(connection)

            # [5단계] DELETE 실행 - 레코드 삭제
            self.db_adapter.execute_delete(cursor, new_id)
            # DELETE 카운터 증가
            if perf_counter:
                perf_counter.increment_delete()
            # DELETE 커밋 (삭제 영구 반영)
            self.db_adapter.commit(connection)

            # 전체 CRUD 사이클 레이턴시 계산 (밀리초 단위)
            latency_ms = (time.time() - start_time) * 1000
            # 트랜잭션 완료 기록 (TPS 및 레이턴시 통계용)
            if perf_counter:
                perf_counter.record_transaction(latency_ms)
            # 워커별 트랜잭션 카운트 증가
            self.transaction_count += 1
            return True
        except Exception as e:
            # 에러 발생 시 로그 기록
            self.log_error("Transaction", str(e))
            # 에러 카운터 증가
            if perf_counter:
                perf_counter.increment_error()
            # 트랜잭션 롤백 (미완료 변경사항 취소)
            self.db_adapter.rollback(connection)
            return False
        finally:
            # 커서 정리 (리소스 해제)
            if cursor:
                try:
                    cursor.close()
                except:
                    # 커서 닫기 실패 시 무시
                    pass

    def run(self) -> int:
        """
        워커 실행 (향상된 재연결 로직 포함)

        개선사항:
        1. DB 재기동 시 자동 재연결 로직 강화
        2. Exponential Backoff 적용 (100ms → 200ms → 400ms → 800ms → 5000ms)
        3. 연속 실패 시 백오프 적용으로 DB 과부하 방지
        4. 커넥션 유효성 검사로 손상된 커넥션 자동 교체
        5. 상세 로깅으로 문제 추적 용이

        Returns:
            완료된 트랜잭션 수
        """
        logger.info(f"[{self.thread_name}] Starting (mode: {self.mode})")

        connection = None
        consecutive_errors = 0  # 연속 에러 카운트 (백오프 트리거용)
        max_id = self.max_id_cache

        while datetime.now() < self.end_time:
            # 우아한 종료 요청 확인
            if shutdown_handler and shutdown_handler.is_shutdown_requested():
                break

            # TPS 속도 제한 (Rate Limiting)
            if self.rate_limiter and not self.rate_limiter.acquire(timeout=0.5):
                continue

            try:
                # 커넥션 없는 경우: 새 커넥션 획득 시도
                if connection is None:
                    now = time.time()
                    if now - self.last_error_log_time > 5: # 5초마다 로그
                         logger.warning(
                             f"[{self.thread_name}] Waiting for connection... "
                             f"(Pool: {self.db_adapter.get_pool_stats().get('pool_total', '?')})"
                         )
                         self.last_error_log_time = now

                    # 유효한 커넥션 획득 시도 (내부 재시도 로직 포함)
                    connection = self._get_valid_connection()

                    if connection is not None:
                        # 커넥션 획득 성공: 에러 카운터 및 백오프 리셋
                        consecutive_errors = 0
                        self.reset_backoff()
                    else:
                        # 커넥션 획득 실패: 연속 에러 카운트 증가
                        consecutive_errors += 1
                        if consecutive_errors >= 2:
                            # 연속 2회 이상 실패 시 백오프 적용
                            # DB 재기동 등 일시적 연결 불가 시 과부하 방지
                            logger.warning(
                                f"[{self.thread_name}] {consecutive_errors} consecutive failures. "
                                f"Retrying after {self.current_backoff_ms}ms backoff..."
                            )
                            time.sleep(self.current_backoff_ms / 1000.0)
                            self.current_backoff_ms = min(self.current_backoff_ms * 2, self.MAX_BACKOFF_MS)
                        else:
                            # 첫 실패는 1초 대기 후 재시도
                            time.sleep(1)
                        continue
                else:
                    # 커넥션이 있는 경우: 유효성 검사
                    if not self._is_connection_valid(connection):
                        # 손상된 커넥션: 폐기 및 새 커넥션 획득
                        self.db_adapter.discard_connection(connection)
                        connection = self._get_valid_connection()
                        if perf_counter:
                            perf_counter.increment_connection_recreate()

                # SELECT/UPDATE/DELETE/MIXED 모드: 기존 데이터 필요
                needs_data = self.mode in [WorkMode.SELECT_ONLY, WorkMode.UPDATE_ONLY,
                                           WorkMode.DELETE_ONLY, WorkMode.MIXED]
                if needs_data and (max_id == 0 or self.transaction_count % 100 == 0):
                    if connection:
                        cursor = connection.cursor()
                        max_id = self.db_adapter.get_max_id(cursor)
                        cursor.close()
                    if max_id == 0:
                        time.sleep(1)
                        continue

                # 모드별 DB 작업 실행
                if self.mode == WorkMode.INSERT_ONLY:
                    success = self.execute_insert(connection)
                elif self.mode == WorkMode.SELECT_ONLY:
                    success = self.execute_select(connection, max_id)
                elif self.mode == WorkMode.UPDATE_ONLY:
                    success = self.execute_update(connection, max_id)
                elif self.mode == WorkMode.DELETE_ONLY:
                    success = self.execute_delete(connection, max_id)
                elif self.mode == WorkMode.MIXED:
                    success = self.execute_mixed(connection, max_id)
                else:
                    success = self.execute_full(connection)

                # 작업 실패 처리
                if not success:
                    consecutive_errors += 1
                    if consecutive_errors >= 2:
                        # 연속 2회 이상 실패 시 커넥션 폐기 및 재시도
                        self.db_adapter.discard_connection(connection)
                        connection = None
                        if perf_counter:
                            perf_counter.increment_connection_recreate()
                        logger.warning(
                            f"[{self.thread_name}] Operation failed. "
                            f"Retrying after {self.current_backoff_ms}ms backoff..."
                        )
                        time.sleep(self.current_backoff_ms / 1000.0)
                        self.current_backoff_ms = min(self.current_backoff_ms * 2, self.MAX_BACKOFF_MS)
                else:
                    consecutive_errors = 0
                    self.reset_backoff()

            except Exception as e:
                self.log_error("Connection", str(e))
                if perf_counter:
                    perf_counter.increment_error()
                if connection:
                    self.db_adapter.discard_connection(connection)
                    connection = None
                    if perf_counter:
                        perf_counter.increment_connection_recreate()
                time.sleep(self.current_backoff_ms / 1000.0)
                self.current_backoff_ms = min(self.current_backoff_ms * 2, self.MAX_BACKOFF_MS)

        if connection:
            self.db_adapter.release_connection(connection)

        logger.info(f"[{self.thread_name}] Completed. Transactions: {self.transaction_count}")
        return self.transaction_count


# ============================================================================
# 모니터링 스레드 - Enhanced
# ============================================================================
class MonitorThread(threading.Thread):
    """모니터링 스레드

    주기적으로 테스트 진행 상황을 로그로 출력합니다.
    TPS, 레이턴시, 커넥션 풀 상태 등을 모니터링합니다.
    """

    def __init__(self, interval_seconds: float, end_time: datetime,
                 sub_second_interval_ms: int, db_adapter: DatabaseAdapter):
        """MonitorThread 초기화

        Args:
            interval_seconds: 모니터링 출력 간격 (초)
            end_time: 테스트 종료 시간
            sub_second_interval_ms: Sub-second TPS 측정 윈도우 (밀리초)
            db_adapter: 데이터베이스 어댑터 (풀 상태 조회용)
        """
        super().__init__(name="Monitor", daemon=True)
        self.interval_seconds = interval_seconds
        self.end_time = end_time
        self.sub_second_interval_ms = sub_second_interval_ms
        self.db_adapter = db_adapter
        self.running = True
        self.warmup_end_logged = False

    def run(self):
        """모니터링 메인 루프

        interval_seconds 간격으로 성능 통계를 로그로 출력합니다.
        Graceful shutdown 요청 시 또는 테스트 종료 시 중단됩니다.
        """
        logger.info(f"[Monitor] Starting (interval: {self.interval_seconds}s)")

        while self.running and datetime.now() < self.end_time:
            if shutdown_handler and shutdown_handler.is_shutdown_requested():
                break

            time.sleep(self.interval_seconds)

            # perf_counter가 초기화되지 않은 경우 스킵
            if perf_counter is None:
                continue

            interval_stats = perf_counter.get_interval_stats()
            stats = perf_counter.get_stats()
            latency_stats = perf_counter.get_latency_stats()
            pool_stats = self.db_adapter.get_pool_stats()

            realtime_tps = perf_counter.get_sub_second_tps()
            is_warmup = perf_counter.is_warmup_period()
            has_warmup = perf_counter.has_warmup_config()

            if has_warmup and not is_warmup and not self.warmup_end_logged:
                self.warmup_end_logged = True
                logger.info("=" * 80)
                logger.info("[Monitor] *** WARMUP COMPLETED *** Starting measurement phase...")
                logger.info("=" * 80)

            status_indicator = "[WARMUP]  " if is_warmup else "[RUNNING] "

            if is_warmup:
                avg_tps_str = "-"
            elif has_warmup:
                avg_tps_str = f"{round(stats['post_warmup_tps'])}"
            else:
                avg_tps_str = f"{round(stats['avg_tps'])}"

            logger.info(
                f"[Monitor] {status_indicator}"
                f"TXN: {interval_stats['interval_transactions']:,} | "
                f"INS: {interval_stats['interval_inserts']:,} | "
                f"SEL: {interval_stats['interval_selects']:,} | "
                f"UPD: {interval_stats['interval_updates']:,} | "
                f"DEL: {interval_stats['interval_deletes']:,} | "
                f"ERR: {interval_stats['interval_errors']:,} | "
                f"Avg TPS: {avg_tps_str} | "
                f"RT TPS: {round(realtime_tps)} | "
                f"Lat(p50/p95/p99): {latency_stats['p50']:.1f}/{latency_stats['p95']:.1f}/{latency_stats['p99']:.1f}ms | "
                f"Pool: {pool_stats.get('pool_active', 0)}/{pool_stats.get('pool_total', 0)}"
            )

            # 시계열 데이터 기록
            perf_counter.record_time_series(pool_stats)

        logger.info("[Monitor] Stopped")

    def stop(self):
        """모니터링 스레드 중지 요청"""
        self.running = False


# ============================================================================
# 부하 테스터 메인 클래스
# ============================================================================
class MultiDBLoadTester:
    """멀티 데이터베이스 부하 테스터

    Oracle, PostgreSQL, MySQL, SQL Server, Tibero, DB2 등
    다양한 데이터베이스에 대한 부하 테스트를 수행합니다.
    """

    def __init__(self, config: DatabaseConfig):
        """MultiDBLoadTester 초기화

        Args:
            config: 데이터베이스 연결 및 테스트 설정
        """
        self.config = config
        self.db_adapter = self._create_adapter()

    def _create_adapter(self) -> DatabaseAdapter:
        """설정에 맞는 데이터베이스 어댑터 생성

        Returns:
            해당 DB 타입의 어댑터 인스턴스
        """
        db_type = self.config.db_type.lower()

        adapters = {
            'oracle': OracleJDBCAdapter,
            'postgresql': PostgreSQLJDBCAdapter, 'postgres': PostgreSQLJDBCAdapter, 'pg': PostgreSQLJDBCAdapter,
            'mysql': MySQLJDBCAdapter,
            'singlestore': SingleStoreJDBCAdapter,
            'sqlserver': SQLServerJDBCAdapter, 'mssql': SQLServerJDBCAdapter,
            'tibero': TiberoJDBCAdapter,
            'db2': DB2JDBCAdapter
        }

        if db_type not in adapters:
            raise ValueError(f"Unsupported database type: {self.config.db_type}")

        return adapters[db_type](self.config.jre_dir)

    def print_ddl(self):
        """테이블 생성 DDL 출력

        해당 데이터베이스 타입에 맞는 DDL을 콘솔에 출력합니다.
        """
        print("\n" + "=" * 80)
        print(f"DDL for {self.config.db_type.upper()} (JDBC)")
        print("=" * 80)
        print(self.db_adapter.get_ddl())
        print("=" * 80 + "\n")

    def run_load_test(self, thread_count: int, duration_seconds: int,
                      mode: str = WorkMode.FULL, skip_schema_setup: bool = False,
                      truncate_table: bool = False,
                      monitor_interval: float = 1.0, sub_second_interval_ms: int = 100,
                      warmup_seconds: int = 30, ramp_up_seconds: int = 0,
                      target_tps: int = 0, batch_size: int = 1,
                      output_format: Optional[str] = None, output_file: Optional[str] = None):
        """부하 테스트 실행"""
        global perf_counter, shutdown_handler

        logger.info(f"Starting load test: {thread_count} threads for {duration_seconds}s (mode: {mode})")

        # 우아한 종료 핸들러 초기화
        shutdown_handler = GracefulShutdown()

        # 성능 카운터 초기화
        perf_counter = PerformanceCounter(sub_second_window_ms=sub_second_interval_ms)

        # 커넥션 풀 생성
        self.db_adapter.create_connection_pool(self.config)

        # 스키마 설정 (기존 스키마가 있으면 재사용)
        if not skip_schema_setup:
            logger.info("Setting up database schema...")
            conn = self.db_adapter.get_connection()
            try:
                self.db_adapter.setup_schema(conn)
            except Exception as e:
                logger.error(f"Schema setup failed: {e}")
                self.db_adapter.release_connection(conn)
                sys.exit(1)
            finally:
                self.db_adapter.release_connection(conn)

        if truncate_table:
            logger.info("Truncating table...")
            conn = self.db_adapter.get_connection()
            if not conn:
                logger.error("Failed to get connection for table truncation")
                sys.exit(1)
            try:
                self.db_adapter.truncate_table(conn)
            except Exception as e:
                logger.error(f"Table truncate failed: {e}")
                self.db_adapter.release_connection(conn)
                sys.exit(1)
            finally:
                self.db_adapter.release_connection(conn)

        # 기존 데이터 확인
        max_id_cache = 0
        if mode in [WorkMode.SELECT_ONLY, WorkMode.UPDATE_ONLY, WorkMode.DELETE_ONLY, WorkMode.MIXED]:
            conn = self.db_adapter.get_connection()
            if conn:
                cursor = conn.cursor()
                max_id_cache = self.db_adapter.get_max_id(cursor)
                cursor.close()
                self.db_adapter.release_connection(conn)
            logger.info(f"Found {max_id_cache} existing records")

        # 시간 설정
        now = datetime.now()
        warmup_end_time = now + timedelta(seconds=warmup_seconds) if warmup_seconds > 0 else None
        ramp_up_end_time = (warmup_end_time or now) + timedelta(seconds=ramp_up_seconds) if ramp_up_seconds > 0 else None
        end_time = now + timedelta(seconds=duration_seconds + warmup_seconds)

        # 워밍업 설정
        if warmup_seconds > 0 and perf_counter and warmup_end_time:
            perf_counter.set_warmup_end_time(warmup_end_time.timestamp())
            logger.info("=" * 80)
            logger.info("Warmup period: %s seconds (Avg TPS will be calculated after warmup)", warmup_seconds)
            logger.info(
                "Total test duration: %s seconds (warmup) + %s seconds (measurement) = %s seconds",
                warmup_seconds, duration_seconds, warmup_seconds + duration_seconds
            )
            logger.info("=" * 80)
        else:
            logger.info("=" * 80)
            logger.info("No warmup period. Test duration: %s seconds", duration_seconds)
            logger.info("=" * 80)

        # TPS 속도 제한기
        rate_limiter = RateLimiter(target_tps) if target_tps > 0 else None
        if target_tps > 0:
            logger.info(f"Target TPS: {target_tps}")

        # 모니터링 스레드
        monitor = MonitorThread(
            interval_seconds=monitor_interval,
            end_time=end_time,
            sub_second_interval_ms=sub_second_interval_ms,
            db_adapter=self.db_adapter
        )
        monitor.start()

        # Ramp-up 적용 워커 실행
        total_transactions = 0
        ramp_up_delay = ramp_up_seconds / thread_count if ramp_up_seconds > 0 else 0

        with ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="Worker") as executor:
            futures = []
            for i in range(thread_count):
                # Ramp-up 간격 대기
                if ramp_up_delay > 0 and i > 0:
                    time.sleep(ramp_up_delay)
                    if shutdown_handler.is_shutdown_requested():
                        break

                worker = LoadTestWorker(
                    worker_id=i + 1,
                    db_adapter=self.db_adapter,
                    end_time=end_time,
                    mode=mode,
                    max_id_cache=max_id_cache,
                    batch_size=batch_size,
                    rate_limiter=rate_limiter,
                    ramp_up_end_time=ramp_up_end_time
                )
                future = executor.submit(worker.run)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    result = future.result()
                    total_transactions += result
                except Exception as e:
                    logger.error(f"Worker failed: {str(e)}")

        monitor.stop()
        monitor.join(timeout=5)

        # 최종 통계 출력
        self._print_final_stats(thread_count, duration_seconds, total_transactions, mode,
                                warmup_seconds, target_tps, batch_size,
                                now, warmup_end_time, end_time)

        # 결과 내보내기
        if output_format and output_file:
            self._export_results(output_format, output_file, thread_count, duration_seconds, mode)

        self.db_adapter.close_pool()

    def _print_final_stats(self, thread_count: int, duration_seconds: int,
                           total_transactions: int, mode: str,
                           warmup_seconds: int, target_tps: int, batch_size: int,
                           start_time: datetime, warmup_end_time: Optional[datetime], end_time: datetime):
        """최종 통계 출력

        테스트 완료 후 최종 성능 통계를 로그로 출력합니다.

        Args:
            thread_count: 워커 스레드 수
            duration_seconds: 테스트 실행 시간 (초)
            total_transactions: 총 트랜잭션 수
            mode: 작업 모드
            warmup_seconds: 워밍업 시간 (초)
            target_tps: 목표 TPS
            batch_size: 배치 크기
        """
        if not perf_counter:
            return
        final_stats = perf_counter.get_stats()
        latency_stats = perf_counter.get_latency_stats()

        logger.info("=" * 80)
        logger.info("LOAD TEST COMPLETED - FINAL STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Database Type: {self.config.db_type.upper()} (JDBC)")
        logger.info(f"Work Mode: {mode}")
        logger.info(f"Total Threads: {thread_count}")
        logger.info(f"Test Duration: {duration_seconds}s (Warmup: {warmup_seconds}s)")
        if target_tps > 0:
            logger.info(f"Target TPS: {target_tps}")
        if batch_size > 1:
            logger.info(f"Batch Size: {batch_size}")
        logger.info(f"Start Time (Warmup): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if warmup_end_time:
            logger.info(f"Start Time (Running): {warmup_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logger.info(f"Start Time (Running): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End Time (Running): {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("-" * 80)
        logger.info(f"Total Transactions: {final_stats['total_transactions']:,}")
        logger.info(f"  - Inserts: {final_stats['total_inserts']:,}")
        logger.info(f"  - Selects: {final_stats['total_selects']:,}")
        logger.info(f"  - Updates: {final_stats['total_updates']:,}")
        logger.info(f"  - Deletes: {final_stats['total_deletes']:,}")
        logger.info(f"Total Errors: {final_stats['total_errors']:,}")
        logger.info(f"Verification Failures: {final_stats['verification_failures']:,}")
        logger.info("-" * 80)
        logger.info(f"Average TPS (overall): {round(final_stats['avg_tps'])}")
        if warmup_seconds > 0:
            logger.info(f"Average TPS (post-warmup): {round(final_stats['post_warmup_tps'])}")
        logger.info(f"Realtime TPS (last 1s): {round(final_stats['realtime_tps'])}")

        if final_stats['total_transactions'] > 0:
            success_rate = (1 - final_stats['total_errors'] / final_stats['total_transactions']) * 100
            logger.info(f"Success Rate: {success_rate:.2f}%")

        logger.info("-" * 80)
        logger.info(f"Latency (ms) - Avg: {latency_stats['avg']:.2f}, P50: {latency_stats['p50']:.2f}, "
                    f"P95: {latency_stats['p95']:.2f}, P99: {latency_stats['p99']:.2f}, "
                    f"Min: {latency_stats['min']:.2f}, Max: {latency_stats['max']:.2f}")
        logger.info("=" * 80)

    def _export_results(self, output_format: str, output_file: str,
                        thread_count: int, duration_seconds: int, mode: str):
        """결과 파일 내보내기

        테스트 결과를 지정된 형식(CSV/JSON)으로 파일에 저장합니다.

        Args:
            output_format: 출력 형식 (csv 또는 json)
            output_file: 출력 파일 경로
            thread_count: 워커 스레드 수
            duration_seconds: 테스트 실행 시간 (초)
            mode: 작업 모드
        """
        if not perf_counter:
            return
        stats = perf_counter.get_stats()
        latency_stats = perf_counter.get_latency_stats()
        time_series = perf_counter.time_series

        config_dict = {
            'db_type': self.config.db_type,
            'host': self.config.host,
            'thread_count': thread_count,
            'duration_seconds': duration_seconds,
            'mode': mode
        }

        if output_format.lower() == 'csv':
            ResultExporter.export_csv(output_file, stats, time_series, config_dict)
        elif output_format.lower() == 'json':
            ResultExporter.export_json(output_file, stats, time_series, config_dict, latency_stats)


# ============================================================================
# 명령행 인자 파싱
# ============================================================================
def parse_arguments():
    """명령행 인자 파싱

    CLI에서 입력된 인자들을 파싱하여 argparse.Namespace 객체로 반환합니다.

    Returns:
        파싱된 명령행 인자를 담은 Namespace 객체
    """
    parser = argparse.ArgumentParser(
        description='Multi-Database Load Tester v2.4 (JDBC)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Work Modes:
  full        : INSERT -> SELECT -> UPDATE -> DELETE (default)
  insert-only : INSERT -> COMMIT only
  select-only : SELECT only (requires existing data)
  update-only : UPDATE only (requires existing data)
  delete-only : DELETE only (requires existing data)
  mixed       : INSERT 60%, SELECT 20%, UPDATE 15%, DELETE 5%

사용 예시:
  # 기본 사용법
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --thread-count 200

  # 워밍업 및 Ramp-up 적용
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --warmup 30 --ramp-up 60

  # 배치 INSERT 및 TPS 제한
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --mode insert-only --batch-size 100 --target-tps 5000

  # 결과 내보내기
  python multi_db_load_tester_jdbc.py --db-type oracle \\
      --host localhost --port 1521 --sid XEPDB1 \\
      --user test --password pass --output-format json --output-file results.json
        """
    )

    # 필수 옵션
    parser.add_argument('--db-type', required=True,
                        choices=['oracle', 'postgresql', 'postgres', 'pg', 'mysql', 'singlestore', 'sqlserver', 'mssql', 'tibero', 'db2'])
    parser.add_argument('--host', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)

    # 연결 옵션
    parser.add_argument('--port', type=int)
    parser.add_argument('--database')
    parser.add_argument('--sid')
    parser.add_argument('--service-name')
    parser.add_argument('--jre-dir', default='./jre')

    # 풀 설정
    parser.add_argument('--min-pool-size', type=int, default=100)
    parser.add_argument('--max-pool-size', type=int, default=200)
    parser.add_argument('--connection-timeout', type=int, default=10,
                        help='Connection/Read timeout in seconds (default: 10)')

    # 커넥션 풀 고급 설정
    parser.add_argument('--max-lifetime', type=int, default=1800,
                        help='Connection max lifetime in seconds (default: 1800 = 30min)')
    parser.add_argument('--leak-detection-threshold', type=int, default=60,
                        help='Leak detection threshold in seconds (default: 60)')
    parser.add_argument('--idle-check-interval', type=int, default=30,
                        help='Idle connection health check interval in seconds (default: 30)')
    parser.add_argument('--idle-timeout', type=int, default=30,
                        help='Idle connection timeout in seconds (default: 30)')
    parser.add_argument('--keepalive-time', type=int, default=30,
                        help='Keepalive interval for idle connections in seconds (default: 30, min: 30)')

    # 테스트 설정
    parser.add_argument('--thread-count', type=int, default=100)
    parser.add_argument('--test-duration', type=int, default=300)
    parser.add_argument('--mode', choices=[WorkMode.FULL, WorkMode.INSERT_ONLY, WorkMode.SELECT_ONLY,
                                           WorkMode.UPDATE_ONLY, WorkMode.DELETE_ONLY, WorkMode.MIXED],
                        default=WorkMode.FULL)
    parser.add_argument('--skip-schema-setup', action='store_true')
    parser.add_argument('--truncate', action='store_true',
                        help='Truncate table before test (clears data, resets identity/sequence)')

    # 고급 설정
    parser.add_argument('--warmup', type=int, default=30, help='Warmup period in seconds')
    parser.add_argument('--ramp-up', type=int, default=0, help='Ramp-up period in seconds')
    parser.add_argument('--target-tps', type=int, default=0, help='Target TPS (0 = unlimited)')
    parser.add_argument('--batch-size', type=int, default=1, help='Batch INSERT size')

    # 모니터링
    parser.add_argument('--monitor-interval', type=float, default=1.0)
    parser.add_argument('--sub-second-interval', type=int, default=100)

    # 결과 내보내기
    parser.add_argument('--output-format', choices=['csv', 'json'])
    parser.add_argument('--output-file')

    # 기타
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--print-ddl', action='store_true')
    parser.add_argument('--version', action='store_true', help='Show version and exit')

    return parser.parse_args()


# ============================================================================
# 메인 함수
# ============================================================================
def main():
    """프로그램 진입점

    명령행 인자를 파싱하고 부하 테스트를 실행합니다.
    JVM 초기화, 데이터베이스 연결, 워커 스레드 관리 등을 수행합니다.
    """
    if '--version' in sys.argv:
        print(f"Multi-Database Load Tester v{VERSION} (JDBC)")
        return

    args = parse_arguments()

    if not JAYDEBEAPI_AVAILABLE:
        logger.error("jaydebeapi/JPype1 not installed")
        sys.exit(1)

    logger.setLevel(getattr(logging, args.log_level))

    if args.version:
        print(f"Multi-Database Load Tester v{VERSION} (JDBC)")
        return

    config = DatabaseConfig(
        db_type=args.db_type, host=args.host, port=args.port,
        database=args.database, sid=args.sid, service_name=args.service_name,
        user=args.user, password=args.password,
        min_pool_size=args.min_pool_size, max_pool_size=args.max_pool_size,
        jre_dir=args.jre_dir,
        max_lifetime_seconds=args.max_lifetime,
        leak_detection_threshold_seconds=args.leak_detection_threshold,
        idle_check_interval_seconds=args.idle_check_interval,
        idle_timeout_seconds=args.idle_timeout,
        keepalive_time_seconds=args.keepalive_time,
        connection_timeout_seconds=args.connection_timeout
    )

    # JVM 초기화
    initialize_jvm(args.jre_dir)

    if not os.path.exists(args.jre_dir):
        logger.error(f"JRE directory not found: {args.jre_dir}")
        sys.exit(1)

    try:
        tester = MultiDBLoadTester(config)
    except Exception as e:
        logger.error(f"Failed to create tester: {str(e)}")
        sys.exit(1)

    if args.print_ddl:
        tester.print_ddl()
        return

    # 설정 출력
    logger.info("=" * 80)
    logger.info(f"MULTI-DATABASE LOAD TESTER v{VERSION} (JDBC)")
    logger.info("=" * 80)
    logger.info(f"Database: {config.db_type.upper()} @ {config.host}")
    logger.info(f"Threads: {args.thread_count} | Duration: {args.test_duration}s | Mode: {args.mode}")
    if args.ramp_up > 0:
        logger.info(f"Ramp-up: {args.ramp_up}s")
    if args.target_tps > 0:
        logger.info(f"Target TPS: {args.target_tps}")
    if args.batch_size > 1:
        logger.info(f"Batch Size: {args.batch_size}")
    logger.info("=" * 80)

    try:
        tester.run_load_test(
            thread_count=args.thread_count,
            duration_seconds=args.test_duration,
            mode=args.mode,
            skip_schema_setup=args.skip_schema_setup,
            truncate_table=args.truncate,
            monitor_interval=args.monitor_interval,
            sub_second_interval_ms=args.sub_second_interval,
            warmup_seconds=args.warmup,
            ramp_up_seconds=args.ramp_up,
            target_tps=args.target_tps,
            batch_size=args.batch_size,
            output_format=args.output_format,
            output_file=args.output_file
        )
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
