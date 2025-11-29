# 빠른 시작 가이드 v2.0 (Quick Start Guide)

## 5분 안에 시작하기 - 5개 데이터베이스 지원

### 1단계: 환경 설정

```bash
# 프로젝트 디렉터리로 이동
cd your_project_directory

# 가상환경 생성
python3 -m venv venv

# 가상환경 활성화
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 패키지 설치
pip install -r requirements_v2.txt
```

### 2단계: 데이터베이스별 추가 설정

#### SQL Server (신규)
ODBC Driver 17 for SQL Server 설치 필요

**Windows:**
[다운로드 링크](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

**Linux (Ubuntu/Debian):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

#### Tibero (신규)
특별한 추가 설치 불필요 (oracledb 드라이버 사용)

### 3단계: 데이터베이스 스키마 생성

#### Oracle
```bash
sqlplus username/password@service_name @oracle_schema.sql
```

#### PostgreSQL
```bash
psql -U username -d database -f postgresql_schema.sql
```

#### MySQL
```bash
mysql -u username -p database < mysql_schema.sql
```

#### SQL Server (신규)
```bash
sqlcmd -S server -d database -U username -P password -i sqlserver_schema.sql
```

#### Tibero (신규)
```bash
tbsql username/password@service_name @tibero_schema.sql
```

### 4단계: 부하 테스트 실행

#### Oracle
```bash
python multi_db_load_tester_v2.py \
    --db-type oracle \
    --host "your_dsn" \
    --user your_user \
    --password your_password \
    --thread-count 100 \
    --test-duration 60
```

#### PostgreSQL
```bash
python multi_db_load_tester_v2.py \
    --db-type postgresql \
    --host localhost \
    --port 5432 \
    --database testdb \
    --user your_user \
    --password your_password \
    --thread-count 100 \
    --test-duration 60
```

#### MySQL
```bash
python multi_db_load_tester_v2.py \
    --db-type mysql \
    --host localhost \
    --port 3306 \
    --database testdb \
    --user your_user \
    --password your_password \
    --thread-count 100 \
    --test-duration 60
```

#### SQL Server (신규)
```bash
python multi_db_load_tester_v2.py \
    --db-type sqlserver \
    --host localhost \
    --port 1433 \
    --database testdb \
    --user sa \
    --password your_password \
    --thread-count 100 \
    --test-duration 60
```

#### Tibero (신규)
```bash
python multi_db_load_tester_v2.py \
    --db-type tibero \
    --host "localhost:8629/tibero" \
    --user your_user \
    --password your_password \
    --thread-count 100 \
    --test-duration 60
```

### 스크립트 실행 방법

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

## DDL만 보고 싶을 때

```bash
# Oracle
python multi_db_load_tester_v2.py --db-type oracle --print-ddl

# PostgreSQL
python multi_db_load_tester_v2.py --db-type postgresql --print-ddl

# MySQL
python multi_db_load_tester_v2.py --db-type mysql --print-ddl

# SQL Server (신규)
python multi_db_load_tester_v2.py --db-type sqlserver --print-ddl

# Tibero (신규)
python multi_db_load_tester_v2.py --db-type tibero --print-ddl
```

## 데이터베이스별 특징 요약

| 데이터베이스 | 드라이버 | 기본 포트 | 특이사항 |
|-------------|---------|----------|---------|
| Oracle | oracledb | 1521 | SEQUENCE 사용 |
| PostgreSQL | psycopg2 | 5432 | BIGSERIAL 사용 |
| MySQL | mysql-connector | 3306 | 풀당 최대 32개 제한 |
| **SQL Server** | **pyodbc** | **1433** | **ODBC 드라이버 필요** |
| **Tibero** | **oracledb** | **8629** | **Oracle 호환** |

## 문제 해결 빠른 참조

### SQL Server
```bash
# ODBC 드라이버 설치 확인
odbcinst -j

# 사용 가능한 드라이버 목록
odbcinst -q -d
```

### Tibero
```bash
# Tibero 리스너 확인
ps -ef | grep tblistener

# Tibero 인스턴스 확인
ps -ef | grep tbsvr

# 리스너 재시작
tbdown -l && tbboot -l
```

### 공통 문제

#### 커넥션 에러
```bash
# 더 많은 로그 확인
python multi_db_load_tester_v2.py ... --log-level DEBUG
```

#### 낮은 TPS
1. 데이터베이스 리소스 확인 (CPU, 메모리, I/O)
2. 네트워크 지연 확인
3. 인덱스 상태 확인
4. 통계 업데이트

## 결과 확인

### 실행 중 (5초마다)
```
[Monitor] Stats - Inserts: 12,450 | Selects: 12,450 | Errors: 0 | 
Ver.Fail: 0 | Conn.Recreate: 0 | Avg TPS: 2490.00 | 
Interval TPS: 2490.00 | Elapsed: 5.0s
```

### 테스트 완료
```
================================================================================
LOAD TEST COMPLETED - FINAL STATISTICS
================================================================================
Database Type: SQLSERVER
Total Threads: 100
Test Duration: 60 seconds
Total Inserts: 149,650
Total Selects: 149,650
Total Errors: 0
Average TPS: 2494.17
Success Rate: 100.00%
================================================================================
```

## 다음 단계

- 📖 자세한 내용: `README_v2.md` 참조
- 🔧 성능 튜닝: README_v2.md의 "성능 튜닝" 섹션
- 📊 모니터링: README_v2.md의 "데이터베이스별 모니터링" 섹션

## 도움말

```bash
python multi_db_load_tester_v2.py --help
```

## 파일 목록 (v2.0)

### 핵심
- `multi_db_load_tester_v2.py` - 메인 프로그램 (5개 DB 지원)
- `requirements_v2.txt` - 의존성

### 문서
- `README_v2.md` - 전체 문서
- `QUICKSTART_v2.md` - 이 파일

### DDL
- `oracle_schema.sql`
- `postgresql_schema.sql`
- `mysql_schema.sql`
- **`sqlserver_schema.sql`** (신규)
- **`tibero_schema.sql`** (신규)

### 실행 스크립트
- `run_oracle_test.sh`
- `run_postgresql_test.sh`
- `run_mysql_test.sh`
- **`run_sqlserver_test.sh`** (신규)
- **`run_tibero_test.sh`** (신규)

---

## v2.0 주요 변경사항

### 🎉 신규 지원 데이터베이스
1. **SQL Server 2016+**
   - pyodbc 드라이버 사용
   - IDENTITY를 통한 자동 증가
   - RANGE 파티션 (HASH 파티션 흉내)
   - OUTPUT 절로 ID 반환

2. **Tibero 6+**
   - Oracle 호환 DBMS
   - oracledb 드라이버 사용
   - SEQUENCE 및 HASH 파티션 지원
   - Oracle과 동일한 문법

### 🔧 기술적 개선
- 커넥션 풀: SQL Server용 Queue 기반 풀 구현
- 에러 처리: 데이터베이스별 최적화된 에러 복구
- 모니터링: 5개 DB 모두 지원

---

**Happy Testing with 5 Databases! 🚀**
