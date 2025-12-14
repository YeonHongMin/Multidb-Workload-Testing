# Multi-Database Load Tester v2.0 (JDBC Version)

Oracle, PostgreSQL, MySQL, SQL Server, Tibero를 지원하는 고성능 멀티스레드 데이터베이스 부하 테스트 도구

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

`./jre` 디렉터리에 각 데이터베이스의 JDBC 드라이버를 배치합니다:

```
./jre/
├── oracle/
│   └── ojdbc10.jar
├── tibero/
│   └── tibero7-jdbc.jar
├── postgresql/
│   └── postgresql-42.7.0.jar
├── mysql/
│   └── mysql-connector-j-8.0.33.jar
└── sqlserver/
    └── mssql-jdbc-12.4.0.jre11.jar
```

## 사용법

### 기본 사용법

```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test_user --password test_pass \
    --thread-count 100 --test-duration 60
```

### 작업 모드 (--mode)

| 모드 | 설명 | 사용 사례 |
|------|------|----------|
| `full` | INSERT → COMMIT → SELECT (기본값) | 데이터 무결성 검증 |
| `insert-only` | INSERT → COMMIT만 | 최대 쓰기 처리량 측정 |
| `select-only` | SELECT만 | 읽기 성능 측정 |
| `update-only` | UPDATE → COMMIT | 업데이트 성능 측정 |
| `delete-only` | DELETE → COMMIT | 삭제 성능 측정 |
| `mixed` | INSERT/UPDATE/DELETE 혼합 (6:3:1) | 실제 워크로드 시뮬레이션 |

```bash
# Insert-only 모드 (최대 쓰기 성능)
python multi_db_load_tester_jdbc.py --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --mode insert-only --thread-count 200

# Mixed 모드 (실제 워크로드 시뮬레이션)
python multi_db_load_tester_jdbc.py --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --mode mixed --thread-count 200
```

### 고급 기능 예제

#### 워밍업 + Ramp-up + Rate Limiting

```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost --port 5432 --database testdb \
    --user test --password pass \
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
    --mode insert-only \
    --batch-size 100 \
    --thread-count 50
```

#### 결과 내보내기

```bash
# JSON 형식으로 결과 저장
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --output-format json \
    --output-file results/test_result.json

# CSV 형식으로 결과 저장
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost --port 1521 --sid XEPDB1 \
    --user test --password pass \
    --output-format csv \
    --output-file results/test_result.csv
```

### 데이터베이스별 예제

#### Oracle
```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host 192.168.0.100 --port 1521 --sid ORCL \
    --user test_user --password pass \
    --thread-count 200 --test-duration 300
```

#### PostgreSQL
```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost --port 5432 --database testdb \
    --user test_user --password pass \
    --thread-count 200
```

#### MySQL
```bash
python multi_db_load_tester_jdbc.py \
    --db-type mysql \
    --host localhost --port 3306 --database testdb \
    --user root --password pass \
    --thread-count 100
```

#### SQL Server
```bash
python multi_db_load_tester_jdbc.py \
    --db-type sqlserver \
    --host localhost --port 1433 --database testdb \
    --user sa --password pass \
    --thread-count 200
```

#### Tibero
```bash
python multi_db_load_tester_jdbc.py \
    --db-type tibero \
    --host 192.168.0.140 --port 8629 --sid tibero \
    --user test_user --password pass \
    --thread-count 200
```

## 명령행 옵션

### 필수 옵션

| 옵션 | 설명 |
|------|------|
| `--db-type` | 데이터베이스 타입 (oracle, postgresql, mysql, sqlserver, tibero) |
| `--host` | 데이터베이스 호스트 |
| `--user` | 사용자명 |
| `--password` | 비밀번호 |

### 연결 옵션

| 옵션 | 설명 |
|------|------|
| `--port` | 포트 번호 |
| `--database` | 데이터베이스명 (PostgreSQL, MySQL, SQL Server) |
| `--sid` | SID/서비스명 (Oracle, Tibero) |
| `--jre-dir` | JDBC 드라이버 디렉터리 (기본값: ./jre) |

### 테스트 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--thread-count` | 100 | 워커 스레드 수 |
| `--test-duration` | 300 | 테스트 시간 (초) |
| `--mode` | full | 작업 모드 |
| `--skip-schema-setup` | false | 스키마 생성 스킵 |

### 워밍업 및 부하 제어

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--warmup` | 0 | 워밍업 기간 (초), 통계에서 제외 |
| `--ramp-up` | 0 | 점진적 부하 증가 기간 (초) |
| `--target-tps` | 0 | 목표 TPS 제한 (0=무제한) |
| `--batch-size` | 1 | 배치 INSERT 크기 |

### 결과 출력

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--output-format` | none | 결과 형식 (none, csv, json) |
| `--output-file` | - | 결과 파일 경로 |

### 모니터링 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--monitor-interval` | 5.0 | 모니터 출력 간격 (초) |
| `--sub-second-interval` | 100 | Sub-second 측정 윈도우 (ms) |

### 풀 설정

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--min-pool-size` | 100 | 최소 풀 크기 |
| `--max-pool-size` | 200 | 최대 풀 크기 |

### 기타

| 옵션 | 설명 |
|------|------|
| `--print-ddl` | DDL 스크립트 출력 후 종료 |
| `--log-level` | 로그 레벨 (DEBUG, INFO, WARNING, ERROR) |

## 모니터링 출력 예시

```
[Monitor] Stats - TXN: 45,230 | INS: 45,230 | SEL: 45,230 | UPD: 0 | DEL: 0 | ERR: 0 |
Avg TPS: 1507.67 | RT TPS: 1523.00 | Int TPS: 1518.40 | 100ms TPS: 1520.00 |
Lat(avg/p50/p95/p99): 2.1/1.8/4.5/8.2ms | Pool: 95/100 | Elapsed: 30.0s
```

| 지표 | 설명 |
|------|------|
| TXN | 총 트랜잭션 수 |
| INS | 총 INSERT 수 |
| SEL | 총 SELECT 수 |
| UPD | 총 UPDATE 수 |
| DEL | 총 DELETE 수 |
| ERR | 총 에러 수 |
| Avg TPS | 평균 TPS (전체 기간) |
| RT TPS | 실시간 TPS (최근 1초) |
| Int TPS | 구간 TPS (모니터 간격) |
| 100ms TPS | Sub-second TPS (지정 윈도우) |
| Lat | 레이턴시 (avg/p50/p95/p99) |
| Pool | 커넥션 풀 상태 (사용중/전체) |

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

| DB | 드라이버 | PK 생성 | 파티셔닝 |
|----|---------|---------|----------|
| Oracle | ojdbc | SEQUENCE | HASH 16개 |
| PostgreSQL | postgresql | BIGSERIAL | HASH 16개 |
| MySQL | mysql-connector | AUTO_INCREMENT | HASH 16개 |
| SQL Server | mssql-jdbc | IDENTITY | - |
| Tibero | tibero-jdbc | SEQUENCE | HASH 16개 |

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
3. 커넥션 풀 정리
4. 최종 통계 출력 및 결과 저장

## 문제 해결

### JVM 초기화 실패
- JAVA_HOME 환경 변수 설정 확인
- Java JDK 17+ 설치 확인

### JDBC 드라이버 찾을 수 없음
- `./jre` 디렉터리 구조 확인
- JAR 파일명 패턴 확인 (ojdbc*.jar 등)

### 커넥션 풀 부족
- `--max-pool-size` 값 증가
- 데이터베이스 max_connections 설정 확인

### TPS가 목표치에 도달하지 않음
- `--thread-count` 증가
- `--target-tps` 설정 확인 (0으로 설정 시 무제한)
- 데이터베이스 리소스 확인

## 라이선스

MIT License
