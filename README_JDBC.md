# Multi-Database Load Tester (JDBC Version)

JDBC 드라이버를 사용하는 멀티 데이터베이스 부하 테스트 프로그램

## 주요 특징

- ✅ **JDBC 드라이버 사용**: ./jre 디렉터리의 JDBC JAR 파일 사용
- ✅ **5개 데이터베이스 지원**: Oracle, PostgreSQL, MySQL, SQL Server, Tibero
- ✅ **Java 기반 연결**: JayDeBeApi를 통한 JDBC 연결
- ✅ **고성능 멀티스레딩**: 100~1000개 동시 세션 지원
- ✅ **커넥션 풀링**: 커스텀 Queue 기반 커넥션 풀
- ✅ **자동 에러 복구**: 커넥션 에러 시 자동 재연결
- ✅ **실시간 모니터링**: TPS, 에러율, 처리량 실시간 추적

## 시스템 요구사항

- Python 3.10 이상
- Java Runtime Environment (JRE) 8 이상
- 각 데이터베이스별 JDBC 드라이버 JAR 파일

## 설치 방법

### 1. Python 패키지 설치

```bash
# 가상환경 생성 (권장)
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 패키지 설치
pip install -r requirements_jdbc.txt
```

### 2. JDBC 드라이버 준비

#### 디렉터리 구조
```
project/
├── multi_db_load_tester_jdbc.py
├── requirements_jdbc.txt
└── jre/
    ├── oracle/
    │   └── ojdbc8.jar
    ├── tibero/
    │   └── tibero6-jdbc.jar
    ├── postgresql/
    │   └── postgresql-42.7.3.jar
    ├── mysql/
    │   └── mysql-connector-java-8.0.33.jar
    └── sqlserver/
        └── mssql-jdbc-12.6.1.jre11.jar
```

#### JDBC 드라이버 다운로드 위치

**Oracle JDBC:**
- 다운로드: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
- 파일명: ojdbc8.jar (JDK 8), ojdbc11.jar (JDK 11+)
- 위치: `./jre/oracle/ojdbc8.jar`

**Tibero JDBC:**
- 다운로드: Tibero 설치 디렉터리의 client/lib
- 파일명: tibero6-jdbc.jar 또는 tibero7-jdbc.jar
- 위치: `./jre/tibero/tibero6-jdbc.jar`

**PostgreSQL JDBC:**
- 다운로드: https://jdbc.postgresql.org/download/
- 파일명: postgresql-42.x.x.jar
- 위치: `./jre/postgresql/postgresql-42.7.3.jar`

**MySQL JDBC:**
- 다운로드: https://dev.mysql.com/downloads/connector/j/
- 파일명: mysql-connector-java-8.x.x.jar
- 위치: `./jre/mysql/mysql-connector-java-8.0.33.jar`

**SQL Server JDBC:**
- 다운로드: https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server
- 파일명: mssql-jdbc-12.x.x.jre11.jar
- 위치: `./jre/sqlserver/mssql-jdbc-12.6.1.jre11.jar`

## 사용 방법

### Oracle 예제

```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost \
    --port 1521 \
    --sid XEPDB1 \
    --user test_user \
    --password password123 \
    --thread-count 200 \
    --test-duration 300 \
    --jre-dir ./jre
```

### PostgreSQL 예제

```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost \
    --port 5432 \
    --database testdb \
    --user test_user \
    --password password123 \
    --thread-count 200 \
    --test-duration 300 \
    --jre-dir ./jre
```

### MySQL 예제

```bash
python multi_db_load_tester_jdbc.py \
    --db-type mysql \
    --host localhost \
    --port 3306 \
    --database testdb \
    --user test_user \
    --password password123 \
    --thread-count 100 \
    --test-duration 300 \
    --jre-dir ./jre
```

### SQL Server 예제

```bash
python multi_db_load_tester_jdbc.py \
    --db-type sqlserver \
    --host localhost \
    --port 1433 \
    --database testdb \
    --user sa \
    --password password123 \
    --thread-count 200 \
    --test-duration 300 \
    --jre-dir ./jre
```

### Tibero 예제

```bash
python multi_db_load_tester_jdbc.py \
    --db-type tibero \
    --host localhost \
    --port 8629 \
    --sid tibero \
    --user test_user \
    --password password123 \
    --thread-count 200 \
    --test-duration 300 \
    --jre-dir ./jre
```

## 명령행 옵션

### 필수 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--db-type` | 데이터베이스 타입 | oracle, postgresql, mysql, sqlserver, tibero |
| `--host` | 데이터베이스 호스트 | localhost |
| `--user` | 사용자명 | test_user |
| `--password` | 비밀번호 | password123 |

### 데이터베이스별 필수 옵션

**Oracle/Tibero:**
- `--sid`: SID 또는 Service Name

**PostgreSQL/MySQL/SQL Server:**
- `--database`: 데이터베이스 이름

### 선택 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--port` | - | 포트 번호 (기본값: Oracle 1521, PostgreSQL 5432, MySQL 3306, SQL Server 1433, Tibero 8629) |
| `--jre-dir` | ./jre | JDBC 드라이버 JAR 파일 디렉터리 |
| `--min-pool-size` | 100 | 최소 풀 크기 |
| `--max-pool-size` | 200 | 최대 풀 크기 |
| `--thread-count` | 100 | 워커 스레드 수 |
| `--test-duration` | 300 | 테스트 시간 (초) |
| `--log-level` | INFO | 로그 레벨 (DEBUG, INFO, WARNING, ERROR) |
| `--print-ddl` | - | DDL만 출력하고 종료 |

## DDL 출력

각 데이터베이스별 테이블 생성 DDL을 출력:

```bash
# Oracle DDL
python multi_db_load_tester_jdbc.py --db-type oracle --print-ddl

# PostgreSQL DDL
python multi_db_load_tester_jdbc.py --db-type postgresql --print-ddl

# MySQL DDL
python multi_db_load_tester_jdbc.py --db-type mysql --print-ddl

# SQL Server DDL
python multi_db_load_tester_jdbc.py --db-type sqlserver --print-ddl

# Tibero DDL
python multi_db_load_tester_jdbc.py --db-type tibero --print-ddl
```

## 데이터베이스별 JDBC URL 형식

프로그램이 자동으로 생성하는 JDBC URL 형식:

| 데이터베이스 | JDBC URL 형식 |
|-------------|--------------|
| Oracle | `jdbc:oracle:thin:@host:port:sid` |
| Tibero | `jdbc:tibero:thin:@host:port:sid` |
| PostgreSQL | `jdbc:postgresql://host:port/database` |
| MySQL | `jdbc:mysql://host:port/database` |
| SQL Server | `jdbc:sqlserver://host:port;databaseName=database` |

## 문제 해결

### 1. JDBC 드라이버를 찾을 수 없음

**에러 메시지:**
```
ERROR - Oracle JDBC driver not found in ./jre directory
```

**해결 방법:**
1. JDBC JAR 파일이 `./jre` 디렉터리에 있는지 확인
2. 파일명이 패턴과 일치하는지 확인 (예: ojdbc*.jar)
3. `--jre-dir` 옵션으로 다른 디렉터리 지정

### 2. JPype 에러

**에러 메시지:**
```
JVMNotFoundException: No JVM shared library file (jvm.dll) found.
```

**해결 방법:**
1. Java (JRE 또는 JDK) 설치 확인
2. JAVA_HOME 환경 변수 설정
3. JPype1 재설치: `pip install --upgrade JPype1`

**Windows:**
```bash
set JAVA_HOME=C:\Program Files\Java\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%
```

**Linux/Mac:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$JAVA_HOME/bin:$PATH
```

### 3. 커넥션 에러

**일반적인 원인:**
- 데이터베이스 서버가 실행 중이지 않음
- 방화벽이 포트를 차단
- 잘못된 호스트/포트/사용자명/비밀번호
- 데이터베이스가 원격 접속을 허용하지 않음

**디버깅:**
```bash
# 디버그 모드로 실행
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost \
    --sid XEPDB1 \
    --user test_user \
    --password password123 \
    --log-level DEBUG \
    --thread-count 1 \
    --test-duration 10
```

### 4. 메모리 에러

**에러 메시지:**
```
java.lang.OutOfMemoryError: Java heap space
```

**해결 방법:**
JVM 힙 메모리 증가:

```python
# multi_db_load_tester_jdbc.py 상단에 추가
import jpype
jpype.startJVM(jpype.getDefaultJVMPath(), "-Xmx4g")  # 4GB 힙 메모리
```

## 성능 튜닝

### 1. 스레드 수 조정

```bash
# 낮은 부하로 시작
--thread-count 50

# 점진적 증가
--thread-count 100
--thread-count 200
--thread-count 500
```

### 2. 커넥션 풀 크기

```bash
# 풀 크기 = 스레드 수 ~ 스레드 수 × 1.5
--thread-count 200 \
--min-pool-size 200 \
--max-pool-size 300
```

### 3. 데이터베이스별 최적화

**Oracle/Tibero:**
```sql
-- SEQUENCE 캐시 증가
ALTER SEQUENCE LOAD_TEST_SEQ CACHE 10000;

-- 통계 수집
EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'LOAD_TEST');
```

**PostgreSQL:**
```sql
-- 통계 수집
ANALYZE load_test;

-- 설정 조정
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET work_mem = '256MB';
```

**MySQL:**
```sql
-- 버퍼 풀 증가
SET GLOBAL innodb_buffer_pool_size = 2147483648; -- 2GB
```

**SQL Server:**
```sql
-- 통계 업데이트
UPDATE STATISTICS load_test WITH FULLSCAN;
```

## 모니터링

### 실시간 출력 예시

```
[Monitor] Stats - Inserts: 12,450 | Selects: 12,450 | Errors: 0 | 
Ver.Fail: 0 | Conn.Recreate: 0 | Avg TPS: 2490.00 | 
Interval TPS: 2490.00 | Elapsed: 5.0s
```

### 최종 결과 예시

```
================================================================================
LOAD TEST COMPLETED - FINAL STATISTICS
================================================================================
Database Type: ORACLE (JDBC)
Total Threads: 200
Test Duration: 300 seconds
Total Inserts: 749,650
Total Selects: 749,650
Total Errors: 0
Average TPS: 2498.83
Success Rate: 100.00%
================================================================================
```

## JDBC vs Python Native Driver 비교

| 특징 | JDBC | Python Native |
|------|------|---------------|
| 설치 | JDBC JAR 필요 | pip install로 간단 |
| 성능 | 약간 느림 (JNI 오버헤드) | 더 빠름 |
| 호환성 | 모든 JDBC 드라이버 지원 | DB별 드라이버 필요 |
| 안정성 | 매우 안정적 (Java 표준) | 드라이버 의존적 |
| 기업 환경 | 선호됨 (표준화) | 환경에 따라 다름 |

## 라이선스

MIT License

## 작성자

Database Engineer with expertise in Java, C++, Python

---

## 빠른 시작 체크리스트

- [ ] Python 3.10+ 설치 확인
- [ ] Java (JRE/JDK) 설치 확인
- [ ] `pip install -r requirements_jdbc.txt` 실행
- [ ] JDBC 드라이버 JAR 파일을 `./jre` 디렉터리에 배치
- [ ] 데이터베이스 스키마 생성 (DDL 실행)
- [ ] 테스트 실행

## 지원

문제가 발생하면:
1. `--log-level DEBUG` 옵션으로 실행
2. 로그 파일 확인: `multi_db_load_test_jdbc.log`
3. JDBC 드라이버 버전 확인
4. Java 버전 확인: `java -version`
