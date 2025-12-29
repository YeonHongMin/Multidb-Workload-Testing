# API 문서화
# 멀티 데이터베이스 워크로드 테스트 도구

## 문서 정보
| 항목 | 설명 |
|------|-------------|
| 문서 버전 | 1.0 |
| 프로젝트 이름 | 멀티 데이터베이스 워크로드 테스트 도구 |
| 현재 버전 | v2.3 |
| 최종 업데이트 | 2025-12-29 |
| 문서 관리자 | 개발팀 |

---

## 1. 개요

이 문서는 멀티 데이터베이스 워크로드 테스트 도구의 API 인터페이스를 상세하게 설명합니다. 현재 이 도구는 명령줄 인터페이스(CLI)를 통해 제공되며, 향후 REST API를 추가할 예정입니다.

### 1.1 API 유형
- **CLI API**: 현재 버전(v2.3)에서 제공되는 명령줄 인터페이스
- **REST API**: 향후 버전(v3.0+)에서 계획된 웹 기반 API

---

## 2. CLI API

### 2.1 명령 구조

```bash
python multi_db_load_tester_jdbc.py [필수 인수] [옵션]
```

### 2.2 필수 인수

#### 2.2.1 `--db-type`
**설명**: 테스트할 데이터베이스 유형

**유형**:
- `oracle`: Oracle 데이터베이스
- `postgresql`: PostgreSQL 데이터베이스
- `mysql`: MySQL 데이터베이스
- `sqlserver`: Microsoft SQL Server
- `tibero`: Tibero 데이터베이스
- `db2`: IBM DB2 데이터베이스

**예시**:
```bash
python multi_db_load_tester_jdbc.py --db-type oracle ...
```

#### 2.2.2 `--host`
**설명**: 데이터베이스 서버 호스트 이름 또는 IP 주소

**형식**: 문자열

**예시**:
```bash
python multi_db_load_tester_jdbc.py --host 192.168.1.100 ...
```

#### 2.2.3 `--user`
**설명**: 데이터베이스 사용자 이름

**형식**: 문자열

**예시**:
```bash
python multi_db_load_tester_jdbc.py --user test_user ...
```

#### 2.2.4 `--password`
**설명**: 데이터베이스 비밀번호

**형식**: 문자열

**예시**:
```bash
python multi_db_load_tester_jdbc.py --password secure_password ...
```

---

### 2.3 연결 옵션

#### 2.3.1 `--port`
**설명**: 데이터베이스 포트 번호

**형식**: 정수

**기본값**: 데이터베이스별 기본 포트
- Oracle: 1521
- PostgreSQL: 5432
- MySQL: 3306
- SQL Server: 1433
- Tibero: 8629
- DB2: 50000

**예시**:
```bash
python multi_db_load_tester_jdbc.py --port 5433 ...
```

#### 2.3.2 `--database`
**설명**: 데이터베이스/스키마 이름

**형식**: 문자열

**적용 대상**: PostgreSQL, MySQL, SQL Server, DB2

**예시**:
```bash
python multi_db_load_tester_jdbc.py --database testdb ...
```

#### 2.3.3 `--sid`
**설명**: Oracle/Tibero SID

**형식**: 문자열

**적용 대상**: Oracle, Tibero

**예시**:
```bash
python multi_db_load_tester_jdbc.py --sid ORCL ...
```

#### 2.3.4 `--service-name`
**설명**: Oracle 서비스 이름 (SID 대신 사용)

**형식**: 문자열

**적용 대상**: Oracle

**예시**:
```bash
python multi_db_load_tester_jdbc.py --service-name ORCLPDB1 ...
```

#### 2.3.5 `--jre-dir`
**설명**: JDBC 드라이버가 포함된 JRE 디렉터리 경로

**형식**: 파일 경로

**기본값**: `./jre`

**예시**:
```bash
python multi_db_load_tester_jdbc.py --jre-dir /path/to/jre ...
```

---

### 2.4 풀 구성 옵션

#### 2.4.1 `--min-pool-size`
**설명**: 연결 풀의 최소 크기

**형식**: 정수

**기본값**: 100

**범위**: 1-1000

**예시**:
```bash
python multi_db_load_tester_jdbc.py --min-pool-size 50 ...
```

#### 2.4.2 `--max-pool-size`
**설명**: 연결 풀의 최대 크기

**형식**: 정수

**기본값**: 200

**범위**: 1-1000

**특이사항**: MySQL의 경우 최대 32로 제장됨 (Connector/J 특성)

**예시**:
```bash
python multi_db_load_tester_jdbc.py --max-pool-size 300 ...
```

#### 2.4.3 `--max-lifetime`
**설명**: 연결의 최대 수명(초)

**형식**: 정수

**기본값**: 1800 (30분)

**범위**: 60-86400

**예시**:
```bash
python multi_db_load_tester_jdbc.py --max-lifetime 3600 ...
```

#### 2.4.4 `--leak-detection-threshold`
**설명**: 연결 누수 감지 임계값(초)

**형식**: 정수

**기본값**: 60

**설명**: 연결이 이 시간보다 오래 활성 상태로 유지되면 경고

**예시**:
```bash
python multi_db_load_tester_jdbc.py --leak-detection-threshold 30 ...
```

#### 2.4.5 `--idle-check-interval`
**설명**: 유휴 연결 헬스 체크 간격(초)

**형식**: 정수

**기본값**: 30

**예시**:
```bash
python multi_db_load_tester_jdbc.py --idle-check-interval 60 ...
```

#### 2.4.6 `--idle-timeout`
**설명**: 유휴 연결 타임아웃(초)

**형식**: 정수

**기본값**: 30

**설명**: 유휴 상태인 연결이 풀에서 제거되기까지의 시간

**예시**:
```bash
python multi_db_load_tester_jdbc.py --idle-timeout 60 ...
```

#### 2.4.7 `--keepalive-time`
**설명**: 연결 킵얼라이브 간격(초)

**형식**: 정수

**기본값**: 30

**설명**: 연결의 유효성을 확인하는 간격

**예시**:
```bash
python multi_db_load_tester_jdbc.py --keepalive-time 60 ...
```

#### 2.4.8 `--connection-timeout`
**설명**: 연결 타임아웃(초)

**형식**: 정수

**기본값**: 10

**설명**: 연결 시도의 최대 대기 시간

**예시**:
```bash
python multi_db_load_tester_jdbc.py --connection-timeout 30 ...
```

---

### 2.5 테스트 구성 옵션

#### 2.5.1 `--thread-count`
**설명**: 워커 스레드 수

**형식**: 정수

**기본값**: 100

**범위**: 1-1000

**예시**:
```bash
python multi_db_load_tester_jdbc.py --thread-count 200 ...
```

#### 2.5.2 `--test-duration`
**설명**: 테스트 기간(초)

**형식**: 정수

**기본값**: 300 (5분)

**범위**: 10-86400

**예시**:
```bash
python multi_db_load_tester_jdbc.py --test-duration 600 ...
```

#### 2.5.3 `--mode`
**설명**: 작업 모드

**유형**:
- `full`: 완전한 CRUD 사이클 (INSERT → SELECT → VERIFY → UPDATE → DELETE)
- `insert-only`: 쓰기 성능 테스트만 (INSERT → COMMIT)
- `select-only`: 읽기 성능 테스트만 (SELECT)
- `update-only`: 업데이트 성능 테스트 (UPDATE → COMMIT)
- `delete-only`: 삭제 성능 테스트 (DELETE → COMMIT)
- `mixed`: 실제 워크로드 시뮬레이션 (INSERT 60% / SELECT 20% / UPDATE 15% / DELETE 5%)

**기본값**: `full`

**예시**:
```bash
python multi_db_load_tester_jdbc.py --mode insert-only ...
```

#### 2.5.4 `--skip-schema-setup`
**설명**: 자동 스키마 생성 건너뛰기

**형식**: 플래그 (기본값: false)

**사용법**:
```bash
python multi_db_load_tester_jdbc.py --skip-schema-setup ...
```

#### 2.5.5 `--truncate`
**설명**: 테스트 전 테이블 데이터 모두 삭제

**형식**: 플래그 (기본값: false)

**사용법**:
```bash
python multi_db_load_tester_jdbc.py --truncate ...
```

---

### 2.6 고급 테스트 옵션

#### 2.6.1 `--warmup`
**설명**: 워밍업 기간(초)

**형식**: 정수

**기본값**: 30

**설명**: 워밍업 기간 중의 통계는 평균 TPS 계산에서 제외

**예시**:
```bash
python multi_db_load_tester_jdbc.py --warmup 60 ...
```

#### 2.6.2 `--ramp-up`
**설명**: 램프업 기간(초)

**형식**: 정수

**기본값**: 0 (비활성)

**설명**: 워커 스레드가 점진적으로 활성화되는 기간. 0이면 모든 스레드가 즉시 시작

**예시**:
```bash
python multi_db_load_tester_jdbc.py --ramp-up 60 ...
```

#### 2.6.3 `--target-tps`
**설명**: 목표 TPS 제한

**형식**: 정수

**기본값**: 0 (무제한)

**설명**: 초당 트랜잭션 수를 이 값으로 제한. 토큰 버킷 알고리즘 사용

**예시**:
```bash
python multi_db_load_tester_jdbc.py --target-tps 1000 ...
```

#### 2.6.4 `--batch-size`
**설명**: INSERT 배치 크기

**형식**: 정수

**기본값**: 1

**범위**: 1-1000

**설명**: 배치당 삽입할 레코드 수

**예시**:
```bash
python multi_db_load_tester_jdbc.py --batch-size 100 ...
```

---

### 2.7 모니터링 옵션

#### 2.7.1 `--monitor-interval`
**설명**: 모니터링 출력 간격(초)

**형식**: 실수

**기본값**: 1.0

**범위**: 0.1-60.0

**예시**:
```bash
python multi_db_load_tester_jdbc.py --monitor-interval 2.0 ...
```

#### 2.7.2 `--sub-second-interval`
**설명**: 서브초 측정 윈도우(ms)

**형식**: 정수

**기본값**: 100

**범위**: 50-1000

**설명**: 실시간 TPS 계산을 위한 윈도우 크기

**예시**:
```bash
python multi_db_load_tester_jdbc.py --sub-second-interval 200 ...
```

---

### 2.8 출력 옵션

#### 2.8.1 `--output-format`
**설명**: 결과 출력 형식

**유형**:
- `csv`: CSV 형식으로 결과 내보내기
- `json`: JSON 형식으로 결과 내보내기

**기본값**: 없음 (콘솔만)

**예시**:
```bash
python multi_db_load_tester_jdbc.py --output-format csv ...
```

#### 2.8.2 `--output-file`
**설명**: 출력 파일 경로

**형식**: 파일 경로

**설명**: `--output-format`이 지정된 경우에만 사용

**예시**:
```bash
python multi_db_load_tester_jdbc.py --output-format csv --output-file results.csv ...
```

---

### 2.9 일반 옵션

#### 2.9.1 `--log-level`
**설명**: 로깅 레벨

**유형**:
- `DEBUG`: 상세한 진단 정보
- `INFO`: 일반적인 진행 정보 (기본값)
- `WARNING`: 경고 및 잠재적 문제
- `ERROR`: 오류만

**기본값**: `INFO`

**예시**:
```bash
python multi_db_load_tester_jdbc.py --log-level DEBUG ...
```

#### 2.9.2 `--print-ddl`
**설명**: DDL 문 출력 후 종료

**형식**: 플래그 (기본값: false)

**사용법**:
```bash
python multi_db_load_tester_jdbc.py --print-ddl ...
```

#### 2.9.3 `--version`
**설명**: 버전 정보 표시 후 종료

**형식**: 플래그 (기본값: false)

**사용법**:
```bash
python multi_db_load_tester_jdbc.py --version
```

#### 2.9.4 `--help`
**설명**: 도움말 표시

**형식**: 플래그 (기본값: false)

**사용법**:
```bash
python multi_db_load_tester_jdbc.py --help
```

---

## 3. 사용 예제

### 3.1 기본 사용 예제

#### Oracle 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type oracle \
  --host oracle.example.com \
  --port 1521 \
  --sid ORCL \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300
```

#### PostgreSQL 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type postgresql \
  --host postgres.example.com \
  --port 5432 \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300
```

#### MySQL 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type mysql \
  --host mysql.example.com \
  --port 3306 \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300
```

#### SQL Server 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type sqlserver \
  --host sqlserver.example.com \
  --port 1433 \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300
```

#### Tibero 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type tibero \
  --host tibero.example.com \
  --port 8629 \
  --sid tibero \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300
```

#### DB2 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type db2 \
  --host db2.example.com \
  --port 50000 \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300
```

### 3.2 고급 사용 예제

#### 속도 제한이 있는 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type postgresql \
  --host postgres.example.com \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 200 \
  --test-duration 600 \
  --target-tps 500 \
  --warmup 60 \
  --ramp-up 30
```

#### 배치 INSERT 테스트
```bash
python multi_db_load_tester_jdbc.py \
  --db-type mysql \
  --host mysql.example.com \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 50 \
  --test-duration 300 \
  --mode insert-only \
  --batch-size 100
```

#### 결과 내보내기
```bash
python multi_db_load_tester_jdbc.py \
  --db-type oracle \
  --host oracle.example.com \
  --sid ORCL \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300 \
  --output-format csv \
  --output-file oracle_test_results.csv
```

#### 사용자 정의 풀 구성
```bash
python multi_db_load_tester_jdbc.py \
  --db-type postgresql \
  --host postgres.example.com \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 200 \
  --test-duration 300 \
  --min-pool-size 50 \
  --max-pool-size 250 \
  --max-lifetime 3600 \
  --leak-detection-threshold 30
```

---

## 4. 출력 형식

### 4.1 콘솔 출력

#### 실시간 모니터링
```
14:32:15 - ========================================
14:32:15 - 멀티 데이터베이스 워크로드 테스트 도구 v2.3
14:32:15 - ========================================
14:32:15 - 연결 풀 초기화 중...
14:32:15 - 풀 상태: 100/200 (활성/총)
14:32:16 - 워밍업 중 (30초)...
14:32:26 - 경과: 10초 | TPS: 0.0 | 에러: 0 | 풀: 100/200
14:32:36 - 경과: 20초 | TPS: 0.0 | 에러: 0 | 풀: 100/200
14:32:46 - 워밍업 완료
14:32:46 - 경과: 30초 | TPS: 1250.0 | 에러: 0 | 풀: 100/200
14:32:56 - 경과: 40초 | TPS: 1280.0 | 에러: 0 | 풀: 100/200
...
```

#### 최종 결과
```
========================================
테스트 완료
========================================
총 테스트 시간: 300초 (5분 0초)
총 트랜잭션: 387,500
INSERT 작업: 97,000
SELECT 작업: 96,500
UPDATE 작업: 97,000
DELETE 작업: 97,000
에러 수: 0
검증 실패: 0

성능 메트릭:
평균 TPS: 1,291.7
워밍업 후 TPS: 1,290.2
실시간 TPS (마지막 1초): 1,280.0

레이턴시 통계 (ms):
평균: 77.4
최소: 12.3
최대: 234.5
P50 (중앙값): 72.1
P95: 145.3
P99: 189.7

연결 풀 통계:
총 생성된 연결: 100
재활용된 연결: 387,400
누수 경고: 0
현재 상태: 100/200 (활성/총)
```

### 4.2 CSV 출력 형식

```csv
# 테스트 메타데이터
테스트 타임스탬프,2025-12-29 14:32:15
버전,2.3
데이터베이스,postgresql
호스트,postgres.example.com
포트,5432
데이터베이스 이름,testdb
스레드 수,100
테스트 기간,300
작업 모드,full

# 테스트 구성
최소 풀 크기,100
최대 풀 크기,200
워밍업 기간,30
램프업 기간,0
목표 TPS,0
배치 크기,1

# 최종 통계
경과 시간(초),총 트랜잭션,TPS,INSERT 작업,SELECT 작업,UPDATE 작업,DELETE 작업,에러 수,검증 실패
30,0,0.0,0,0,0,0,0,0
40,12800,1280.0,3200,3200,3200,3200,0,0
50,25600,1280.0,6400,6400,6400,6400,0,0
...
300,387500,1291.7,97000,96500,97000,97000,0,0

# 레이턴시 통계
메트릭,값(ms)
평균,77.4
최소,12.3
최대,234.5
P50,72.1
P95,145.3
P99,189.7

# 연결 풀 통계
메트릭,값
총 생성된 연결,100
재활용된 연결,387400
누수 경고,0
현재 활성 연결,100
현재 총 연결,200
```

### 4.3 JSON 출력 형식

```json
{
  "test_metadata": {
    "timestamp": "2025-12-29 14:32:15",
    "version": "2.3",
    "database": "postgresql",
    "host": "postgres.example.com",
    "port": 5432,
    "database_name": "testdb",
    "thread_count": 100,
    "test_duration": 300,
    "work_mode": "full"
  },
  "test_configuration": {
    "min_pool_size": 100,
    "max_pool_size": 200,
    "warmup_period": 30,
    "ramp_up_period": 0,
    "target_tps": 0,
    "batch_size": 1
  },
  "final_statistics": {
    "total_duration": 300,
    "total_transactions": 387500,
    "average_tps": 1291.7,
    "post_warmup_tps": 1290.2,
    "insert_operations": 97000,
    "select_operations": 96500,
    "update_operations": 97000,
    "delete_operations": 97000,
    "errors": 0,
    "verification_failures": 0,
    "connection_recreations": 0
  },
  "latency_statistics": {
    "average_ms": 77.4,
    "min_ms": 12.3,
    "max_ms": 234.5,
    "p50_ms": 72.1,
    "p95_ms": 145.3,
    "p99_ms": 189.7
  },
  "pool_statistics": {
    "total_created": 100,
    "total_reused": 387400,
    "leak_warnings": 0,
    "active_connections": 100,
    "total_connections": 200
  },
  "time_series_data": [
    {
      "elapsed_seconds": 30,
      "total_transactions": 0,
      "tps": 0.0,
      "insert_operations": 0,
      "select_operations": 0,
      "update_operations": 0,
      "delete_operations": 0,
      "errors": 0,
      "verification_failures": 0,
      "is_warmup": true
    },
    {
      "elapsed_seconds": 40,
      "total_transactions": 12800,
      "tps": 1280.0,
      "insert_operations": 3200,
      "select_operations": 3200,
      "update_operations": 3200,
      "delete_operations": 3200,
      "errors": 0,
      "verification_failures": 0,
      "is_warmup": false
    }
  ]
}
```

---

## 5. 오류 코드

### 5.1 연결 오류

| 오류 코드 | 설명 | 해결 방법 |
|----------|-------------|------------------|
| ERR-001 | 데이터베이스 호스트에 연결할 수 없음 | 호스트 이름과 포트 확인, 네트워크 연결 확인 |
| ERR-002 | 인증 실패 | 사용자 이름과 비밀번호 확인 |
| ERR-003 | 데이터베이스/스키마를 찾을 수 없음 | 데이터베이스 이름 확인 |
| ERR-004 | 연결 타임아웃 | 연결 타임아웃 값 증가, 네트워크 지연 확인 |
| ERR-005 | JDBC 드라이버를 찾을 수 없음 | jre-dir 경로 확인, JDBC 드라이버 파일 확인 |

### 5.2 풀 오류

| 오류 코드 | 설명 | 해결 방법 |
|----------|-------------|------------------|
| ERR-101 | 최소 풀 크기를 생성할 수 없음 | min-pool-size 감소, 데이터베이스 연결 수 확인 |
| ERR-102 | 연결 풀 고갈 | max-pool-size 증가, thread-count 감소 |
| ERR-103 | 연결 누수 감지 | 누스 코드 수정, 연결 적절히 반환 확인 |

### 5.3 테스트 오류

| 오류 코드 | 설명 | 해결 방법 |
|----------|-------------|------------------|
| ERR-201 | 테이블 생성 실패 | 사용자 권한 확인, 데이터베이스 호환성 확인 |
| ERR-202 | 테이블 자르기 실패 | 테이블 잠금 확인, 권한 확인 |
| ERR-203 | 트랜잭션 롤백 | 에러 로그 확인, 데이터 일관성 확인 |

### 5.4 일반 오류

| 오류 코드 | 설명 | 해결 방법 |
|----------|-------------|------------------|
| ERR-901 | 잘못된 인수 | 인수 값 확인, 도움말 (--help) 참조 |
| ERR-902 | JVM 초기화 실패 | Java 설치 확인, JVM 버전 확인 |
| ERR-903 | 파일 쓰기 실패 | 디스크 공간 확인, 파일 권한 확인 |

---

## 6. 향후 REST API (계획)

### 6.1 엔드포인트 (v3.0+)

#### POST /api/tests
새로운 테스트 생성

**요청 본문**:
```json
{
  "database_type": "postgresql",
  "connection": {
    "host": "postgres.example.com",
    "port": 5432,
    "database": "testdb",
    "user": "testuser",
    "password": "testpass"
  },
  "configuration": {
    "thread_count": 100,
    "test_duration": 300,
    "work_mode": "full",
    "warmup": 30,
    "ramp_up": 0,
    "target_tps": 0
  }
}
```

**응답**:
```json
{
  "test_id": "test_20251229_143215",
  "status": "running",
  "start_time": "2025-12-29T14:32:15Z"
}
```

#### GET /api/tests/{test_id}
테스트 상태 조회

**응답**:
```json
{
  "test_id": "test_20251229_143215",
  "status": "running",
  "elapsed_seconds": 45,
  "current_tps": 1280.0,
  "total_transactions": 12800,
  "errors": 0
}
```

#### GET /api/tests/{test_id}/results
테스트 결과 조회

**응답**:
```json
{
  "test_id": "test_20251229_143215",
  "status": "completed",
  "final_statistics": {
    "total_transactions": 387500,
    "average_tps": 1291.7
  },
  "latency_statistics": {
    "average_ms": 77.4,
    "p50_ms": 72.1,
    "p95_ms": 145.3,
    "p99_ms": 189.7
  }
}
```

#### DELETE /api/tests/{test_id}
테스트 중지

**응답**:
```json
{
  "test_id": "test_20251229_143215",
  "status": "stopped",
  "stop_time": "2025-12-29T14:37:00Z"
}
```

---

## 7. 부록

### 7.1 환경 변수

지원되는 환경 변수:

| 변수 | 설명 | 예시 |
|------|-------------|------------------|
| `DB_HOST` | 기본 호스트 | `export DB_HOST=localhost` |
| `DB_USER` | 기본 사용자 이름 | `export DB_USER=testuser` |
| `DB_PASSWORD` | 기본 비밀번호 | `export DB_PASSWORD=testpass` |
| `DB_DATABASE` | 기본 데이터베이스 이름 | `export DB_DATABASE=testdb` |
| `DB_TYPE` | 기본 데이터베이스 유형 | `export DB_TYPE=postgresql` |

### 7.2 스크립트 사용법

각 데이터베이스에 대해 제공되는 스크립트:

#### Oracle
```bash
bash run_oracle_jdbc_test.sh
```

#### PostgreSQL
```bash
bash run_postgresql_jdbc_test.sh
```

#### MySQL
```bash
bash run_mysql_jdbc_test.sh
```

#### SQL Server
```bash
bash run_sqlserver_jdbc_test.sh
```

#### Tibero
```bash
bash run_tibero_jdbc_test.sh
```

#### DB2
```bash
bash run_db2_jdbc_test.sh
```

---

**문서 종료**
