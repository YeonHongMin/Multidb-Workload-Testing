# Multi-Database Load Tester v2.3 (Python & JDBC Driver)

Oracle, PostgreSQL, MySQL, SQL Server, Tibero를 지원하는 고성능 멀티스레드 데이터베이스 부하 테스트 도구

*** DownLoad Link(with JDBC Drivers) : https://drive.google.com/file/d/1Qq_dkVJuYcmU1kBbeQT8Khr0W4Xk4XGs/view?usp=sharing

## 주요 특징

- **5개 데이터베이스 지원**: Oracle, PostgreSQL, MySQL, SQL Server, Tibero
- **JDBC 드라이버 사용**: JayDeBeApi를 통한 통합 JDBC 연결
- **고성능 멀티스레딩**: 최대 1000개 동시 세션 지원
- **6가지 작업 모드**: full, insert-only, select-only, update-only, delete-only, mixed
- **1초 이내 트랜잭션 측정**: Sub-second TPS 실시간 모니터링
- **레이턴시 측정**: P50/P95/P99 응답시간 통계
- **워밍업 기간**: 통계 제외 워밍업 지원
- **점진적 부하 증가**: Ramp-up 기능
- **TPS 제한**: Token Bucket 기반 Rate Limiting
- **배치 INSERT**: 대량 데이터 삽입 최적화
- **결과 내보내기**: CSV/JSON 형식 지원
- **Graceful Shutdown**: Ctrl+C 안전 종료
- **커넥션 풀 모니터링**: 실시간 풀 상태 확인

### v2.3 신규 기능: DB 재기동 시 자동 복구

- **Connection Leak 감지**: 오래 사용 중인 커넥션 자동 감지 및 경고
- **Pool Warm-up**: 초기화 시 min_size 커넥션 미리 생성
- **Connection Max Lifetime**: 오래된 커넥션 자동 갱신
- **Idle Health Check**: 유휴 커넥션 주기적 검증 및 정리
- **🔧 DB 재기동 자동 복구 (버그 수정)**:
  - **커넥션 생성 재시도 로직**: 최대 3회 재시도 + Exponential Backoff (100ms → 200ms → 400ms → 최대 2초)
  - **커넥션 획득 향상**: `acquire()` 메서드 재시도 로직 강화 (백오프 적용, DB listener 과부하 방지)
  - **워커 루프 개선**: 연속 실패 시 백오프 적용, 성공 시 카운터 리셋으로 무한 루프 방지
  - **상세 로깅**: 재시도 횟수, 백오프 시간 등 상세 정보 로그 출력

---

### v2.3 버그 수정 상세

#### 문제: DB 재기동 시 Hang 발생

**증상**:
- DB 재기동 후 커넥션 풀이 전체 손실 (`Pool: 0/0`)
- 워커가 무한 대기 상태에 빠짐 (`TXN: 0`, `RT TPS: 0`)
- DB가 다시 살아도 재연결 시도하지 않음
- 1분 이상 Hang 상태 지속

**원인**:

1. **`_create_connection_internal()` 재시도 부재**
   - 단일 시도 후 바로 `None` 반환
   - DB 재기동 중 연결 실패 시 재시도 없음

2. **`acquire()` 메서드의 재시도 로직 불충분**
   - `retry_count`가 제대로 증가하지 않음
   - `time.sleep(1)` 후 재시도하지 않고 루프 계속됨
   - 백오프 메커니즘 없음

3. **워커 루프의 무한 루프**
   ```python
   if connection is None:
       connection = self._get_valid_connection()
       consecutive_errors = 0  # 무조건 리셋

   if connection is None:
       time.sleep(1)
       continue  # 무한 루프!
   ```

#### 해결책

**1. 커넥션 생성 재시도 로직 추가** (`_create_connection_internal()`)
```python
max_creation_retries = 3
creation_backoff_ms = 100

for attempt in range(max_creation_retries):
    try:
        # 커넥션 생성 시도
        conn = jaydebeapi.connect(...)
        return PooledConnection(connection=conn)
    except Exception as e:
        if attempt < max_creation_retries - 1:
            # 재시도: Exponential Backoff
            logger.warning(f"{attempt + 1}/{max_creation_retries} 시도 실패. {creation_backoff_ms}ms 후 재시도...")
            time.sleep(creation_backoff_ms / 1000.0)
            creation_backoff_ms = min(creation_backoff_ms * 2, 2000)
```

**2. `acquire()` 메서드 재시도 로직 강화**
```python
retry_count = 0
max_retries = 3
backoff_ms = 100

while retry_count < max_retries:
    # 큐 Empty 예외 처리
    except queue.Empty:
        if retry_count < max_retries - 1:
            # 백오프 적용
            time.sleep(backoff_ms / 1000.0)
            backoff_ms = min(backoff_ms * 2, 5000)
        retry_count += 1
```

**3. 워커 루프 개선** (`run()` 메서드)
```python
if connection is None:
    connection = self._get_valid_connection()
    if connection is not None:
        # 성공: 에러 카운터 및 백오프 리셋
        consecutive_errors = 0
        self.reset_backoff()
    else:
        # 실패: 연속 에러 카운트 증가
        consecutive_errors += 1
        if consecutive_errors >= 2:
            # 백오프 적용
            time.sleep(self.current_backoff_ms / 1000.0)
            self.current_backoff_ms = min(self.current_backoff_ms * 2, self.MAX_BACKOFF_MS)
        else:
            time.sleep(1)
        continue
```

#### 테스트 결과 (예상)

DB 재기동 시나리오 테스트:
1. **정상 동작** (재기동 전): TPS ~700-800, 에러 없음
2. **DB 재기동** (12:34:00): 커넥션 손실, 에러 발생
3. **자동 복구** (12:34:05~12:34:20):
   - 워커들이 재시도 시작
   - 백오프 적용으로 DB listener 과부하 방지
   - 새 커넥션 생성 성공
4. **정상 복구** (12:34:20 이후): TPS 복구, 워크로드 계속

**복구 시간**: 약 15-20초 (백오프 포함)

## 시스템 요구사항

- Python 3.10+
- Java JDK 17+ (JVM 필요)
- 지원 데이터베이스:
  - Oracle 19c+
  - PostgreSQL 11+
  - MySQL 5.7+
  - SQL Server 2016+
  - Tibero 6+

## 설치

### 1. Python 패키지 설치

```bash
pip install -r requirements.txt
```

### 2. JDBC 드라이버 배치

JDBC 드라이버는 `./jre/<db_type>/*.jar`에 배치합니다:

```
./jre/
├── db2/
│   └── db2jcc4.jar
├── oracle/
│   └── ojdbc11.jar
├── tibero/
│   └── tibero7-jdbc.jar
├── postgresql/
│   └── postgresql-42.7.0.jar
├── mysql/
│   └── mysql-connector-j-8.0.33.jar
└── sqlserver/
    └── mssql-jdbc-12.4.0.jre11.jar
```

## Linux client/server 실행 절차

이 도구는 "클라이언트(Linux, Python 실행)"에서 실행하며, DB는 별도 "서버(Linux/Unix, DB 구동)"에 있어도 됩니다.

### 1) 서버(DB) 준비

- DB 서버에 대상 DB 설치/구동, 사용자 계정/비밀번호 준비
- 클라이언트에서 DB 포트 접근 가능하도록 방화벽/보안그룹 설정
- 예: Oracle 1521, PostgreSQL 5432, MySQL 3306, SQL Server 1433, Tibero 8629

### 2) 클라이언트 준비 (Linux)

- Python 3.10+, Java JDK 17+ 설치
- 본 리포지토리 클론 또는 파일 복사
- JDBC 드라이버를 `./jre/<db>`에 배치

### 3) (권장) Python 가상환경 구성

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4) 실행

```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host <DB_SERVER_IP> --port 5432 --database testdb \
    --user test_user --password test_pass \
    --thread-count 100 --test-duration 60
```

### 5) (선택) 스크립트로 실행

```bash
chmod +x run_*.sh
./run_postgresql_jdbc_test.sh
```

### 6) 환경 변수 방식 (선택)

`env.example`을 참고해 `.env` 구성 후 실행:

```bash
cp env.example .env
# 필요 값 수정 후 실행
python multi_db_load_tester_jdbc.py --db-type postgresql ...
```

## 사용법

### 기본 사용법

```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test_user --password test_pass \
    --truncate \
    --thread-count 100 --test-duration 60
```

```bash
# Version
python multi_db_load_tester_jdbc.py --version
```

### 작업 모드 (--mode)

| 모드          | 설명                              | 사용 사례                |
| ------------- | --------------------------------- | ------------------------ |
| `full`        | INSERT → COMMIT → SELECT (기본값) | 데이터 무결성 검증       |
| `insert-only` | INSERT → COMMIT만                 | 최대 쓰기 처리량 측정    |
| `select-only` | SELECT만                          | 읽기 성능 측정           |
| `update-only` | UPDATE → COMMIT                   | 업데이트 성능 측정       |
| `delete-only` | DELETE → COMMIT                   | 삭제 성능 측정           |
| `mixed`       | INSERT/UPDATE/DELETE 혼합 (6:3:1) | 실제 워크로드 시뮬레이션 |

```bash
# Insert-only 모드 (최대 쓰기 성능)
python multi_db_load_tester_jdbc.py --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --truncate \
    --mode insert-only --thread-count 200

# Mixed 모드 (실제 워크로드 시뮬레이션)
python multi_db_load_tester_jdbc.py --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --truncate \
    --mode mixed --thread-count 200
```

### 고급 기능 예제

#### 워밍업 + Ramp-up + Rate Limiting

```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost --port 5432 --database testdb \
    --user test --password pass \
    --truncate \
    --warmup 30 \
    --ramp-up 60 \
    --target-tps 5000 \
    --thread-count 200 --test-duration 300
```

#### 배치 INSERT

```bash
python multi_db_load_tester_jdbc.py \
    --db-type mysql \
    --host localhost --port 3306 --database testdb \
    --user root --password pass \
    --truncate \
    --mode insert-only \
    --batch-size 100 \
    --thread-count 50
```

#### 커넥션 풀 고급 설정 (v2.2.2 신규)

```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --truncate \
    --min-pool-size 50 \
    --max-pool-size 100 \
    --max-lifetime 1800 \
    --leak-detection-threshold 60 \
    --idle-check-interval 30
```

#### 결과 내보내기

```bash
# JSON 형식으로 결과 저장
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --truncate \
    --output-format json \
    --output-file results/test_result.json

# CSV 형식으로 결과 저장
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --truncate \
    --output-format csv \
    --output-file results/test_result.csv
```

### 데이터베이스별 예제

#### Oracle

```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host 192.168.0.100 --port 1521 --service-name ORCL \
    --user test_user --password pass \
    --truncate \
    --thread-count 200 --test-duration 300
```

#### PostgreSQL

```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost --port 5432 --database testdb \
    --user test_user --password pass \
    --truncate \
    --thread-count 200
```

#### MySQL

```bash
python multi_db_load_tester_jdbc.py \
    --db-type mysql \
    --host localhost --port 3306 --database testdb \
    --user root --password pass \
    --truncate \
    --thread-count 100
```

> **Note**: MySQL의 커넥션 풀 크기는 기본적으로 32개로 제한됩니다. 이는 MySQL 서버의 max_connections 설정 및 Connector/J 특성을 고려한 것입니다. 자세한 내용은 소스 코드의 `MYSQL_MAX_POOL_SIZE` 상수를 참조하세요.

#### SQL Server

```bash
python multi_db_load_tester_jdbc.py \
    --db-type sqlserver \
    --host localhost --port 1433 --database testdb \
    --user sa --password pass \
    --truncate \
    --thread-count 200
```

#### Tibero

```bash
python multi_db_load_tester_jdbc.py \
    --db-type tibero \
    --host 192.168.0.140 --port 8629 --sid tibero \
    --user test_user --password pass \
    --truncate \
    --thread-count 200
```

## 명령행 옵션

### 필수 옵션

| 옵션         | 설명                                                             |
| ------------ | ---------------------------------------------------------------- |
| `--db-type`  | 데이터베이스 타입 (oracle, postgresql, mysql, sqlserver, tibero) |
| `--host`     | 데이터베이스 호스트                                              |
| `--user`     | 사용자명                                                         |
| `--password` | 비밀번호                                                         |

### 연결 옵션

| 옵션         | 설명                                           |
| ------------ | ---------------------------------------------- |
| `--port`     | 포트 번호                                      |
| `--database` | 데이터베이스명 (PostgreSQL, MySQL, SQL Server) |
| `--sid`         | Oracle/Tibero SID |
| `--service-name`| Oracle service name |
| `--jre-dir`     | JDBC driver directory (default: ./jre) |

### 테스트 옵션

| 옵션                  | 기본값 | 설명             |
| --------------------- | ------ | ---------------- |
| `--thread-count`      | 100    | 워커 스레드 수   |
| `--test-duration`     | 300    | 테스트 시간 (초) |
| `--mode`              | full   | 작업 모드        |
| `--skip-schema-setup` | false  | 스키마 생성 스킵 |

### 워밍업 및 부하 제어

| 옵션           | 기본값 | 설명                            |
| -------------- | ------ | ------------------------------- |
| `--warmup`     | 0      | 워밍업 기간 (초), 통계에서 제외 |
| `--ramp-up`    | 0      | 점진적 부하 증가 기간 (초)      |
| `--target-tps` | 0      | 목표 TPS 제한 (0=무제한)        |
| `--batch-size` | 1      | 배치 INSERT 크기                |

### 결과 출력

| 옵션              | 기본값 | 설명                        |
| ----------------- | ------ | --------------------------- |
| `--output-format` | none   | 결과 형식 (none, csv, json) |
| `--output-file`   | -      | 결과 파일 경로              |

### 모니터링 옵션

| 옵션                    | 기본값 | 설명                        |
| ----------------------- | ------ | --------------------------- |
| `--monitor-interval`    | 5.0    | 모니터 출력 간격 (초)       |
| `--sub-second-interval` | 100    | Sub-second 측정 윈도우 (ms) |

### 풀 설정

| 옵션              | 기본값 | 설명                           |
| ----------------- | ------ | ------------------------------ |
| `--min-pool-size` | 100    | 최소 풀 크기 (Warm-up 시 생성) |
| `--max-pool-size` | 200    | 최대 풀 크기                   |

### 커넥션 풀 고급 설정 (v2.2.2 신규)

| 옵션                         | 기본값 | 설명                               |
| ---------------------------- | ------ | ---------------------------------- |
| `--max-lifetime`             | 1800   | 커넥션 최대 수명 (초, 30분)        |
| `--leak-detection-threshold` | 60     | Leak 감지 임계값 (초)              |
| `--idle-check-interval`      | 30     | 유휴 커넥션 Health Check 주기 (초) |
| `--idle-timeout`             | 30     | 유휴 커넥션 제거 시간 (초)         |
| `--keepalive-time`           | 30     | 유휴 커넥션 검증 주기 (초)         |
| `--connection-timeout`       | 30     | 커넥션 로그인 타임아웃 (초)        |

**Timeout 설정 가이드**

- `--connection-timeout`: DB 재기동/Failover 중 커넥션 획득이 오래 블록되는 것을 방지합니다. HA 환경은 5~30초 권장.
- `--idle-timeout`/`--keepalive-time`: keepalive로 죽은 커넥션을 빠르게 감지하고 idle-timeout으로 정리합니다. `idle-timeout > keepalive-time` 권장.

### 기타

| 옵션          | 설명                                    |
| ------------- | --------------------------------------- |
| `--print-ddl` | DDL 스크립트 출력 후 종료               |
| `--log-level` | 로그 레벨 (DEBUG, INFO, WARNING, ERROR) |

### Logging

- Console format: `HH:MM:SS - <message>`
- File format: `YYYY-MM-DD HH:MM:SS - <message>`
- Main log: `multi_db_load_test_jdbc.log` (INFO and below)
- Error log: `multi_db_load_test_jdbc_error.log` (WARN/ERROR)

## 커넥션 풀 관리 (v2.2.2 신규)

### Pool Warm-up

초기화 시 `min_size`만큼 커넥션을 미리 생성하여 첫 번째 요청부터 최적의 성능을 제공합니다.

```
[Pool Warm-up] Creating 100 initial connections...
[Pool Warm-up] Completed. Created 100/100 connections
```

### Connection Leak 감지

커넥션이 `leak_detection_threshold` 시간 이상 반환되지 않으면 경고를 출력합니다:

```
[Leak Detection] Potential connection leak detected!
Connection held for 65.3s by thread 'Worker-0001' (threshold: 60s)
```

### Connection Max Lifetime

`max_lifetime` 시간이 지난 커넥션은 자동으로 폐기되고 새 커넥션으로 교체됩니다. 이는 데이터베이스 서버의 유휴 연결 타임아웃 문제를 방지합니다.

### Idle Health Check

백그라운드 스레드가 `idle_check_interval` 주기로 유휴 커넥션을 검증합니다:

- 유효하지 않은 커넥션 자동 제거
- 만료된 커넥션 갱신
- 커넥션 풀 상태 유지

```
[Health Check] Checked: 50, Removed: 2, Recycled: 3
```

### 확장된 풀 통계

```python
{
    'pool_total': 100,           # 현재 총 커넥션 수
    'pool_active': 50,           # 사용 중인 커넥션 수
    'pool_idle': 50,             # 유휴 커넥션 수
    'pool_total_created': 120,   # 총 생성된 커넥션 수
    'pool_recycled': 20,         # 재생성된 커넥션 수
    'pool_leak_warnings': 0      # Leak 경고 횟수
}
```

## 모니터링 출력 예시

```
12:34:56 - [Monitor] [RUNNING] TXN: 45,230 | INS: 45,230 | SEL: 45,230 | UPD: 0 | DEL: 0 | ERR: 0 | Avg TPS: 1508 | RT TPS: 1523 | Lat(p95/p99): 4.5/8.2ms | Pool: 95/100
```

| 지표      | 설명                         |
| --------- | ---------------------------- |
| TXN       | 총 트랜잭션 수               |
| INS       | 총 INSERT 수                 |
| SEL       | 총 SELECT 수                 |
| UPD       | 총 UPDATE 수                 |
| DEL       | 총 DELETE 수                 |
| ERR       | 총 에러 수                   |
| Avg TPS   | Average TPS (rounded) |
| RT TPS    | Realtime TPS (rounded) |
| Lat       | Latency p95/p99 (ms) |
| Pool      | 커넥션 풀 상태 (사용중/전체) |

## 결과 파일 형식

### JSON 출력 예시

```json
{
  "test_info": {
    "db_type": "oracle",
    "host": "localhost",
    "mode": "full",
    "thread_count": 100,
    "test_duration": 300,
    "warmup": 30,
    "ramp_up": 60,
    "target_tps": 5000,
    "batch_size": 1
  },
  "summary": {
    "total_transactions": 450000,
    "total_inserts": 450000,
    "total_selects": 450000,
    "total_updates": 0,
    "total_deletes": 0,
    "total_errors": 0,
    "elapsed_seconds": 300.0,
    "average_tps": 1500.0
  },
  "latency": {
    "avg_ms": 2.1,
    "p50_ms": 1.8,
    "p95_ms": 4.5,
    "p99_ms": 8.2,
    "min_ms": 0.5,
    "max_ms": 25.3
  }
}
```

## 데이터베이스별 특징

| DB         | 드라이버        | PK 생성        | 파티셔닝  | 풀 제한   |
| ---------- | --------------- | -------------- | --------- | --------- |
| Oracle     | ojdbc           | SEQUENCE       | HASH 16개 | -         |
| PostgreSQL | postgresql      | BIGSERIAL      | HASH 16개 | -         |
| MySQL      | mysql-connector | AUTO_INCREMENT | HASH 16개 | 최대 32개 |
| SQL Server | mssql-jdbc      | IDENTITY       | -         | -         |
| Tibero     | tibero-jdbc     | SEQUENCE       | HASH 16개 | -         |

## 환경 변수 설정

`env.example` 파일을 `.env`로 복사하여 설정할 수 있습니다:

```bash
# 공통 부하 테스트 설정
MIN_POOL_SIZE=100
MAX_POOL_SIZE=200
THREAD_COUNT=200
TEST_DURATION=300
LOG_LEVEL=INFO

# 커넥션 풀 고급 설정 (선택사항)
MAX_LIFETIME_SECONDS=1800
LEAK_DETECTION_THRESHOLD_SECONDS=60
IDLE_CHECK_INTERVAL_SECONDS=30
```

## 실행 스크립트

```bash
# 권한 부여
chmod +x run_*.sh

# 각 데이터베이스별 실행
./run_oracle_jdbc_test.sh
./run_postgresql_jdbc_test.sh
./run_mysql_jdbc_test.sh
./run_sqlserver_jdbc_test.sh
./run_tibero_jdbc_test.sh
```

## Graceful Shutdown

테스트 중 `Ctrl+C`를 누르면 안전하게 종료됩니다:

1. 모든 워커 스레드에 종료 신호 전송
2. 진행 중인 트랜잭션 완료 대기
3. Health Check 스레드 종료
4. 커넥션 풀 정리 (활성/유휴 커넥션 모두)
5. 최종 통계 출력 및 결과 저장

## 문제 해결

### JVM 초기화 실패

- JAVA_HOME 환경 변수 설정 확인
- Java JDK 17+ 설치 확인

### JDBC 드라이버 찾을 수 없음

- `./jre` 디렉터리 구조 확인
- JAR 파일명 패턴 확인 (ojdbc\*.jar 등)

### 커넥션 풀 부족

- `--max-pool-size` 값 증가
- 데이터베이스 max_connections 설정 확인

### Connection Leak 경고 발생

- 트랜잭션 처리 시간이 `--leak-detection-threshold` 초과
- 긴 트랜잭션이 예상되는 경우 임계값 증가
- 실제 Leak인 경우 코드 검토 필요

### TPS가 목표치에 도달하지 않음

- `--thread-count` 증가
- `--target-tps` 설정 확인 (0으로 설정 시 무제한)
- 데이터베이스 리소스 확인

### MySQL 풀 크기 제한

- MySQL은 기본적으로 최대 32개 커넥션으로 제한됨
- 더 많은 커넥션이 필요한 경우 `MYSQL_MAX_POOL_SIZE` 상수 조정
- MySQL 서버의 `max_connections` 설정도 함께 조정 필요

## 라이선스

MIT License

## Python JDBC Notes

- Added DB2 support (db-type: `db2`, default port: `50000`, JDBC JAR: `./jre/db2/jcc*.jar`).
- New options: `--truncate`, `--idle-timeout`, `--keepalive-time`, `--connection-timeout`.
- Defaults aligned with Java version: warmup `30s`, monitor interval `1.0s`.
