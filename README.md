# Multi-Database Load Tester v2.0

Oracle, PostgreSQL, MySQL, **SQL Server**, **Tibero**를 지원하는 멀티스레드 데이터베이스 부하 테스트 프로그램

## 주요 특징

- ✅ **5개 데이터베이스 지원**: Oracle, PostgreSQL, MySQL, **SQL Server**, **Tibero**
- ✅ **고성능 멀티스레딩**: 100~1000개 동시 세션 지원
- ✅ **커넥션 풀링**: 각 DB에 최적화된 커넥션 풀 사용
- ✅ **자동 에러 복구**: 커넥션 에러 시 자동 재연결
- ✅ **실시간 모니터링**: TPS, 에러율, 처리량 실시간 추적
- ✅ **파티셔닝 지원**: 대용량 처리를 위한 파티션 테이블
- ✅ **트랜잭션 검증**: INSERT → COMMIT → SELECT 패턴으로 데이터 무결성 확인

## 시스템 요구사항

- Python 3.10 이상
- 지원하는 데이터베이스:
  - Oracle 19c 이상
  - PostgreSQL 11 이상 (해시 파티션 지원)
  - MySQL 5.7 이상
  - **SQL Server 2016 이상**
  - **Tibero 6 이상**

## 설치 방법

### 1. 프로젝트 준비

```bash
mkdir db_load_test
cd db_load_test
```

### 2. 가상환경 생성 및 활성화

```bash
python3 -m venv venv

# Linux/Mac
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. 의존성 설치

```bash
# 모든 데이터베이스 드라이버 설치
pip install -r requirements_v2.txt

# 또는 필요한 드라이버만 설치
pip install oracledb          # Oracle & Tibero
pip install psycopg2-binary   # PostgreSQL
pip install mysql-connector-python  # MySQL
pip install pyodbc            # SQL Server
```

### 4. SQL Server 전용 - ODBC 드라이버 설치

#### Windows
[Microsoft ODBC Driver 17 for SQL Server 다운로드](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

#### Linux (Ubuntu/Debian)
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

#### Linux (RHEL/CentOS)
```bash
curl https://packages.microsoft.com/config/rhel/8/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
```

## 데이터베이스 스키마 생성

### DDL 출력

```bash
# Oracle DDL
python multi_db_load_tester_v2.py --db-type oracle --print-ddl

# PostgreSQL DDL
python multi_db_load_tester_v2.py --db-type postgresql --print-ddl

# MySQL DDL
python multi_db_load_tester_v2.py --db-type mysql --print-ddl

# SQL Server DDL
python multi_db_load_tester_v2.py --db-type sqlserver --print-ddl

# Tibero DDL
python multi_db_load_tester_v2.py --db-type tibero --print-ddl
```

### 스키마 생성

#### Oracle
```bash
sqlplus username/password@service @oracle_schema.sql
```

#### PostgreSQL
```bash
psql -U username -d database -f postgresql_schema.sql
```

#### MySQL
```bash
mysql -u username -p database < mysql_schema.sql
```

#### SQL Server
```bash
sqlcmd -S server -d database -U username -P password -i sqlserver_schema.sql
```

#### Tibero
```bash
tbsql username/password@service @tibero_schema.sql
```

## 사용 방법

### Oracle

```bash
python multi_db_load_tester_v2.py \
    --db-type oracle \
    --host "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db-server)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))" \
    --user test_user \
    --password test_password \
    --thread-count 200 \
    --test-duration 300
```

### PostgreSQL

```bash
python multi_db_load_tester_v2.py \
    --db-type postgresql \
    --host localhost \
    --port 5432 \
    --database testdb \
    --user test_user \
    --password test_password \
    --thread-count 200 \
    --test-duration 300
```

### MySQL

```bash
python multi_db_load_tester_v2.py \
    --db-type mysql \
    --host localhost \
    --port 3306 \
    --database testdb \
    --user test_user \
    --password test_password \
    --thread-count 200 \
    --test-duration 300
```

### SQL Server (신규)

```bash
python multi_db_load_tester_v2.py \
    --db-type sqlserver \
    --host localhost \
    --port 1433 \
    --database testdb \
    --user sa \
    --password your_password \
    --thread-count 200 \
    --test-duration 300
```

### Tibero (신규)

```bash
python multi_db_load_tester_v2.py \
    --db-type tibero \
    --host "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=tibero-host)(PORT=8629))(CONNECT_DATA=(SERVICE_NAME=tibero)))" \
    --user test_user \
    --password test_password \
    --thread-count 200 \
    --test-duration 300
```

## 데이터베이스별 특징

| 데이터베이스 | 드라이버 | PK 생성 | 파티셔닝 | 풀 크기 제한 |
|-------------|---------|---------|----------|-------------|
| Oracle | oracledb | SEQUENCE.NEXTVAL | HASH | 무제한 |
| PostgreSQL | psycopg2 | BIGSERIAL | HASH (11+) | 무제한 |
| MySQL | mysql-connector | AUTO_INCREMENT | HASH | 32개/풀 |
| **SQL Server** | **pyodbc** | **IDENTITY** | **RANGE** | **무제한** |
| **Tibero** | **oracledb** | **SEQUENCE** | **HASH** | **무제한** |

## 데이터베이스별 모니터링

### SQL Server (신규)

```sql
-- 활성 세션 모니터링
SELECT 
    session_id, login_name, host_name, program_name,
    status, command, wait_type, wait_time,
    cpu_time, logical_reads, writes
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
WHERE login_name = 'test_user'
ORDER BY session_id;

-- 테이블 통계
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT thread_id) as unique_threads,
    MIN(created_at) as first_insert,
    MAX(created_at) as last_insert
FROM load_test;

-- 파티션별 데이터 분포
SELECT 
    p.partition_number,
    p.rows as row_count,
    fg.name as filegroup_name
FROM sys.partitions p
JOIN sys.allocation_units au ON p.partition_id = au.container_id
JOIN sys.filegroups fg ON au.data_space_id = fg.data_space_id
WHERE p.object_id = OBJECT_ID('load_test')
ORDER BY p.partition_number;

-- 초당 처리량 (최근 1분)
SELECT 
    DATEPART(SECOND, created_at) as second,
    COUNT(*) as inserts_per_second
FROM load_test
WHERE created_at >= DATEADD(MINUTE, -1, GETDATE())
GROUP BY DATEPART(SECOND, created_at)
ORDER BY second DESC;
```

### Tibero (신규)

```sql
-- Tibero는 Oracle과 유사한 모니터링 쿼리 사용

-- 세션 모니터링
SELECT 
    sid, serial#, username, program, 
    sql_id, event, wait_class, seconds_in_wait
FROM v$session 
WHERE username = 'TEST_USER'
ORDER BY sid;

-- 테이블 통계
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT thread_id) as unique_threads,
    MIN(created_at) as first_insert,
    MAX(created_at) as last_insert
FROM LOAD_TEST;

-- 파티션별 데이터 분포
SELECT 
    partition_name,
    num_rows,
    blocks,
    avg_row_len
FROM user_tab_partitions
WHERE table_name = 'LOAD_TEST'
ORDER BY partition_position;
```

## 성능 튜닝

### SQL Server

```sql
-- 1. 인덱스 재구성
ALTER INDEX ALL ON load_test REORGANIZE;

-- 2. 통계 업데이트
UPDATE STATISTICS load_test WITH FULLSCAN;

-- 3. 데이터베이스 설정 확인
SELECT name, value_in_use 
FROM sys.configurations
WHERE name IN ('max degree of parallelism', 'cost threshold for parallelism', 'max server memory (MB)');

-- 4. 대기 통계 확인
SELECT 
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    max_wait_time_ms,
    signal_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_type NOT LIKE '%SLEEP%'
ORDER BY wait_time_ms DESC;
```

### Tibero

```sql
-- 1. SEQUENCE 캐시 크기 조정
ALTER SEQUENCE LOAD_TEST_SEQ CACHE 10000;

-- 2. 통계 수집
EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'LOAD_TEST', CASCADE => TRUE);

-- 3. Tibero 설정 확인 (tbSID.tip 파일)
-- MAX_SESSION_COUNT=1000
-- TOTAL_SHM_SIZE=2G
-- MEMORY_TARGET=4G

-- 4. 세션 대기 이벤트 확인
SELECT event, total_waits, time_waited, average_wait
FROM v$system_event
WHERE event NOT LIKE '%idle%'
ORDER BY time_waited DESC;
```

## 실행 스크립트 사용

```bash
# 실행 권한 부여
chmod +x run_*.sh

# Oracle
./run_oracle_test.sh

# PostgreSQL
./run_postgresql_test.sh

# MySQL
./run_mysql_test.sh

# SQL Server (신규)
./run_sqlserver_test.sh

# Tibero (신규)
./run_tibero_test.sh
```

## 명령행 옵션

### 필수 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--db-type` | 데이터베이스 타입 | oracle, postgresql, mysql, sqlserver, tibero |
| `--host` | 데이터베이스 호스트 | localhost, DSN |
| `--user` | 사용자명 | test_user |
| `--password` | 비밀번호 | test_password |

### PostgreSQL/MySQL/SQL Server 필수 옵션

| 옵션 | 설명 |
|------|------|
| `--database` | 데이터베이스 이름 |

### 선택 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--port` | - | 포트 번호 |
| `--min-pool-size` | 100 | 최소 풀 크기 |
| `--max-pool-size` | 200 | 최대 풀 크기 |
| `--pool-increment` | 10 | 풀 증가량 |
| `--thread-count` | 100 | 워커 스레드 수 |
| `--test-duration` | 300 | 테스트 시간 (초) |
| `--log-level` | INFO | 로그 레벨 |
| `--print-ddl` | - | DDL만 출력 |

## 문제 해결

### SQL Server 관련

#### ODBC 드라이버 찾을 수 없음
```
[IM002] [Microsoft][ODBC Driver Manager] Data source name not found
```
**해결**: ODBC Driver 17 for SQL Server 설치 확인

#### 신뢰할 수 없는 서버 인증서
**해결**: 연결 문자열에 `TrustServerCertificate=yes` 추가 (이미 구현됨)

### Tibero 관련

#### 연결 실패
- Tibero 리스너 실행 확인: `tbdown -l && tbboot -l`
- 포트 확인: 기본 8629
- 서비스명 확인: `$TB_SID`

#### 드라이버 호환성
- Tibero 6: oracledb 1.x 권장
- Tibero 7: oracledb 2.x 지원

## 파일 목록

### 핵심 파일
- `multi_db_load_tester_v2.py` - 메인 프로그램 (5개 DB 지원)
- `requirements_v2.txt` - Python 패키지 의존성

### DDL 스크립트
- `oracle_schema.sql` - Oracle DDL
- `postgresql_schema.sql` - PostgreSQL DDL
- `mysql_schema.sql` - MySQL DDL
- **`sqlserver_schema.sql`** - **SQL Server DDL (신규)**
- **`tibero_schema.sql`** - **Tibero DDL (신규)**

### 실행 스크립트
- `run_oracle_test.sh`
- `run_postgresql_test.sh`
- `run_mysql_test.sh`
- **`run_sqlserver_test.sh`** - **(신규)**
- **`run_tibero_test.sh`** - **(신규)**

## 버전 히스토리

### v2.0.0 (2025-01-15)
- **신규**: SQL Server 지원 추가
- **신규**: Tibero 지원 추가
- 총 5개 데이터베이스 지원

### v1.0.0 (2025-01-15)
- 초기 릴리스 (Oracle, PostgreSQL, MySQL)

## 라이선스

MIT License

## 작성자

Jeremiah (Senior Python Performance Tuning Developer & DBA)

---

## 빠른 참조

### SQL Server 빠른 시작
```bash
# 1. ODBC 드라이버 설치 (Windows/Linux)
# 2. 스키마 생성
sqlcmd -S localhost -d testdb -U sa -P password -i sqlserver_schema.sql
# 3. 부하 테스트
python multi_db_load_tester_v2.py --db-type sqlserver --host localhost --database testdb --user sa --password password --thread-count 100 --test-duration 60
```

### Tibero 빠른 시작
```bash
# 1. Tibero 리스너 시작
tbdown -l && tbboot -l
# 2. 스키마 생성
tbsql user/pass@tibero @tibero_schema.sql
# 3. 부하 테스트
python multi_db_load_tester_v2.py --db-type tibero --host "host:8629/tibero" --user test_user --password test_pass --thread-count 100 --test-duration 60
```
