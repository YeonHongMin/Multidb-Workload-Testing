# 문제 해결 가이드
# 멀티 데이터베이스 워크로드 테스트 도구

## 문서 정보
| 항목 | 설명 |
|------|-------------|
| 문서 버전 | 1.0 |
| 프로젝트 이름 | 멀티 데이터베이스 워크로드 테스트 도구 |
| 현재 버전 | v2.4 |
| 최종 업데이트 | 2025-12-29 |
| 문서 관리자 | 개발팀 |

---

## 1. 개요

이 문서는 멀티 데이터베이스 워크로드 테스트 도구 사용 중 발생할 수 있는 일반적인 문제들과 그 해결 방법을 제공합니다.

---

## 2. 설치 및 설정 문제

### 2.1 JVM 초기화 실패

**증상**:
```
JPype 초기화 오류: JVM을 찾을 수 없거나 초기화할 수 없음
```

**원인**:
- Java가 설치되지 않음
- JAVA_HOME 환경 변수가 설정되지 않음
- 잘못된 Java 버전

**해결 방법**:

1. Java 설치 확인:
```bash
java -version
# Java 17 이상 필요
```

2. JAVA_HOME 설정 (Linux/macOS):
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=$JAVA_HOME/bin:$PATH
```

3. JAVA_HOME 설정 (Windows):
```cmd
set JAVA_HOME=C:\Program Files\Java\jdk-17
set PATH=%JAVA_HOME%\bin;%PATH%
```

4. Python 패키지 재설치:
```bash
pip install --upgrade JPype1 jaydebeapi
```

---

### 2.2 JDBC 드라이버 찾기 실패

**증상**:
```
오류: JDBC 드라이버를 찾을 수 없음: ojdbc10.jar
```

**원인**:
- JDBC 드라이버 파일이 존재하지 않음
- 잘못된 --jre-dir 경로
- 드라이버 파일명이 잘못됨

**해결 방법**:

1. JRE 디렉터리 구조 확인:
```
jre/
├── oracle/
│   └── ojdbc10.jar
├── postgresql/
│   └── postgresql-42.2.9.jar
├── mysql/
│   └── mysql-connector-j-9.5.0-1.el8.noarch.rpm
├── sqlserver/
│   └── mssql-jdbc-13.2.1.jre8.jar
├── tibero/
│   └── tibero7-jdbc.jar
├── db2/
│   └── db2jcc4.jar
└── singlestore/
    └── singlestore-jdbc-1.2.1.jar
```

2. 올바른 경로 지정:
```bash
python multi_db_load_tester_jdbc.py \
  --db-type oracle \
  --jre-dir /path/to/jre \
  ...
```

3. 드라이버 파일 다운로드:
- Oracle: Oracle 공식 웹사이트에서 ojdbc10.jar 다운로드
- PostgreSQL: Maven Central에서 postgresql-*.jar 다운로드
- MySQL: MySQL 공식 웹사이트에서 mysql-connector-j 다운로드

---

### 2.3 의존성 설치 실패

**증상**:
```
ModuleNotFoundError: No module named 'jpype'
```

**해결 방법**:

1. requirements.txt 사용:
```bash
pip install -r requirements.txt
```

2. 개별 패키지 설치:
```bash
pip install JPype1 jaydebeapi
```

3. pip 버전 업그레이드:
```bash
pip install --upgrade pip
pip install --upgrade JPype1 jaydebeapi
```

---

## 3. 연결 문제

### 3.1 호스트 연결 거부

**증상**:
```
오류: 데이터베이스 호스트에 연결할 수 없음: Connection refused
```

**원인**:
- 데이터베이스 서버가 실행 중이 아님
- 잘못된 호스트 이름 또는 IP 주소
- 방화벽 차단
- 포트가 잘못됨

**해결 방법**:

1. 데이터베이스 서버 상태 확인:

Oracle:
```bash
sqlplus / as sysdba
SELECT status FROM v$instance;
```

PostgreSQL:
```bash
pg_isready -h localhost -p 5432
```

MySQL:
```bash
mysqladmin -h localhost -u root -p ping
```

2. 네트워크 연결 확인:
```bash
telnet dbhost 5432
# 또는
nc -zv dbhost 5432
```

3. 방화벽 규칙 확인 (Linux):
```bash
sudo iptables -L -n | grep 5432
```

4. 올바른 포트 사용 확인:
```bash
python multi_db_load_tester_jdbc.py \
  --db-type postgresql \
  --host localhost \
  --port 5432 \
  ...
```

---

### 3.2 인증 실패

**증상**:
```
오류: 인증 실패: FATAL: password authentication failed
```

**원인**:
- 잘못된 사용자 이름 또는 비밀번호
- 사용자가 데이터베이스에 권한이 없음
- 인증 방법이 올바르지 않음

**해결 방법**:

1. 사용자 이름과 비밀번호 확인:
```bash
# 테스트 연결
psql -h localhost -U testuser -d testdb
```

2. 사용자 권한 확인:

PostgreSQL:
```sql
-- 접속 후
\du testuser
```

MySQL:
```sql
-- 접속 후
SHOW GRANTS FOR 'testuser'@'%';
```

3. 필요한 권한 부여:

PostgreSQL:
```sql
GRANT ALL PRIVILEGES ON DATABASE testdb TO testuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO testuser;
```

MySQL:
```sql
GRANT ALL PRIVILEGES ON testdb.* TO 'testuser'@'%';
FLUSH PRIVILEGES;
```

---

### 3.3 연결 타임아웃

**증상**:
```
오류: 연결 타임아웃: Timed out waiting for connection
```

**해결 방법**:

1. 연결 타임아웃 증가:
```bash
python multi_db_load_tester_jdbc.py \
  --connection-timeout 30 \
  ...
```

2. 네트워크 지연 확인:
```bash
ping dbhost
traceroute dbhost
```

3. 데이터베이스 서버 부하 확인:
```bash
# PostgreSQL
SELECT count(*) FROM pg_stat_activity;

# MySQL
SHOW PROCESSLIST;
```

4. 연결 풀 크기 조정:
```bash
python multi_db_load_tester_jdbc.py \
  --min-pool-size 50 \
  --max-pool-size 100 \
  ...
```

---

### 3.4 연결 풀 고갈

**증상**:
```
경고: 연결 풀 고갈 - 활성 연결: 200/200
```

**원인**:
- 너무 많은 동시 요청
- 연결 누수
- max-pool-size가 너무 작음

**해결 방법**:

1. 최대 풀 크기 증가:
```bash
python multi_db_load_tester_jdbc.py \
  --max-pool-size 300 \
  ...
```

2. 스레드 수 감소:
```bash
python multi_db_load_tester_jdbc.py \
  --thread-count 150 \
  ...
```

3. 연결 누수 감지:
```bash
python multi_db_load_tester_jdbc.py \
  --leak-detection-threshold 30 \
  ...
```

4. 로그에서 누스 경고 확인:
```
14:32:45 - WARNING: 연결 누수 감지: 연결이 65초 동안 활성 상태
```

---

## 4. 성능 문제

### 4.1 낮은 TPS

**증상**:
- 예상보다 TPS가 낮음
- 레이턴시가 높음

**원인**:
- 데이터베이스 서터 리소스 부족
- 인덱스 누락
- 네트워크 지연
- 잘못된 구성

**해결 방법**:

1. 데이터베이스 서버 리소스 확인:
```bash
# CPU 사용량
top
htop

# 메모리 사용량
free -h

# 디스크 I/O
iostat -x 1
```

2. 인덱스 확인:
```sql
-- PostgreSQL
EXPLAIN ANALYZE SELECT * FROM test_table WHERE thread_id = 'worker-1';

-- MySQL
EXPLAIN SELECT * FROM test_table WHERE thread_id = 'worker-1';
```

3. 배치 크기 증가:
```bash
python multi_db_load_tester_jdbc.py \
  --batch-size 100 \
  ...
```

4. 스레드 수 조정:
- 너무 적은 스레드 → 증가
- 너무 많은 스레드 → 감소 (경합 발생 가능)

5. 워밍업 기간 증가:
```bash
python multi_db_load_tester_jdbc.py \
  --warmup 60 \
  ...
```

---

### 4.2 높은 레이턴시

**증상**:
- P99 레이턴시가 높음 (>500ms)
- 레이턴시 변동폭이 큼

**해결 방법**:

1. 데이터베이스 쿼리 최적화:
```sql
-- 느린 쿼리 확인
-- PostgreSQL
SELECT * FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;

-- MySQL
SELECT * FROM information_schema.processlist WHERE time > 5;
```

2. 연결 풀 크기 최적화:
```bash
python multi_db_load_tester_jdbc.py \
  --min-pool-size 100 \
  --max-pool-size 200 \
  ...
```

3. 배치 작업 사용:
```bash
python multi_db_load_tester_jdbc.py \
  --batch-size 50 \
  --mode insert-only \
  ...
```

4. 네트워크 지연 확인:
```bash
ping -c 10 dbhost
mtr dbhost
```

5. 데이터베이스 구성 확인:
- WAL/트랜잭션 로그 최적화
- 버퍼 풀 크기 증가
- 병렬 쿼리 활성화

---

### 4.3 메모리 사용량 과다

**증상**:
- Python 프로세스가 많은 메모리 사용
- OOM (Out of Memory) 오류

**해결 방법**:

1. 스레드 수 감소:
```bash
python multi_db_load_tester_jdbc.py \
  --thread-count 100 \
  ...
```

2. 배치 크기 감소:
```bash
python multi_db_load_tester_jdbc.py \
  --batch-size 10 \
  ...
```

3. 모니터링 간격 증가:
```bash
python multi_db_load_tester_jdbc.py \
  --monitor-interval 5.0 \
  ...
```

4. Python 힙 프로파일링:
```bash
python -m memory_profiler multi_db_load_tester_jdbc.py ...
```

---

## 5. 데이터베이스별 문제

### 5.1 Oracle

#### ORA-12541: TNS:no listener

**해결 방법**:
```bash
# 리스너 시작
lsnrctl start

# 리스너 상태 확인
lsnrctl status
```

#### ORA-12514: TNS:listener does not know of service requested

**해결 방법**:
```bash
# SID 대신 서비스 이름 사용
python multi_db_load_tester_jdbc.py \
  --db-type oracle \
  --service-name ORCLPDB1 \
  ...
```

#### ORA-00020: maximum number of processes exceeded

**해결 방법**:
```sql
-- SQL*Plus에서
ALTER SYSTEM SET processes = 500 SCOPE=SPFILE;
ALTER SYSTEM SET sessions = 600 SCOPE=SPFILE;
-- 데이터베이스 재시작
```

---

### 5.2 PostgreSQL

#### connection limit exceeded

**증상**:
```
FATAL: remaining connection slots are reserved
```

**해결 방법**:
```sql
-- max_connections 증가
ALTER SYSTEM SET max_connections = 200;
-- 재시작 필요
```

```bash
# 또는 postgresql.conf 수정
max_connections = 200
```

#### could not create shared memory segment

**해결 방법**:
```bash
# 공유 메모리 설정 증가 (Linux)
sudo sysctl -w kernel.shmmax=17179869184
sudo sysctl -w kernel.shmall=4194304

# 영구 적용을 위해 /etc/sysctl.conf에 추가
```

---

### 5.3 MySQL

#### Too many connections

**해결 방법**:
```sql
-- max_connections 설정 확인
SHOW VARIABLES LIKE 'max_connections';

-- max_connections 증가
SET GLOBAL max_connections = 500;
```

```bash
# 또는 my.cnf 수정
[mysqld]
max_connections = 500
```

#### Connector/J 32 연결 제한

**증상**:
```
MySQL 풀 크기가 32로 제한됨
```

**해결 방법**:
- Connector/J의 특성으로 인해 제한됨
- 여러 테스트 인스턴스 사용:
```bash
# 인스턴스 1
python multi_db_load_tester_jdbc.py --thread-count 32 --max-pool-size 32 ... &

# 인스턴스 2
python multi_db_load_tester_jdbc.py --thread-count 32 --max-pool-size 32 ... &
```

---

### 5.4 SQL Server

#### Timeout expired

**해결 방법**:
```bash
# 연결 타임아웃 증가
python multi_db_load_tester_jdbc.py \
  --connection-timeout 30 \
  ...
```

```sql
-- SQL Server에서 쿼리 타임아웃 증가
EXEC sp_configure 'remote query timeout', 60;
RECONFIGURE;
```

---

### 5.5 Tibero

#### TNS:no listener

**해결 방법**:
```bash
# Tibero 리스너 시작
tbboot listener

# 리스너 상태 확인
tbsql sys/tibero@localhost:8629
```

---

### 5.6 IBM DB2

#### SQL30081N  A communication error has been detected

**해결 방법**:
```bash
# DB2 서비스 확인
db2 list node directory
db2 list admin server

# 포트 확인
netstat -an | grep 50000
```

---

## 6. 테스트 실행 문제

### 6.1 테이블 생성 실패

**증상**:
```
오류: 테이블 생성 실패: permission denied for schema public
```

**해결 방법**:

1. 사용자 권한 확인 및 부여:
```sql
-- PostgreSQL
GRANT CREATE ON SCHEMA public TO testuser;

-- Oracle
GRANT CREATE TABLE TO testuser;
```

2. 스키마 설정 건너뛰기 (테이블이 이미 있는 경우):
```bash
python multi_db_load_tester_jdbc.py \
  --skip-schema-setup \
  ...
```

---

### 6.2 테이블 자르기 실패

**증상**:
```
오류: 테이블 자르기 실패: TRUNCATE TABLE 명령에 대한 권한 부족
```

**해결 방법**:

1. 권한 부여:
```sql
-- PostgreSQL
GRANT TRUNCATE ON test_table TO testuser;

-- MySQL
GRANT DROP, DELETE ON testdb.* TO 'testuser'@'%';

-- Oracle
GRANT DELETE ANY TABLE TO testuser;
```

---

### 6.3 트랜잭션 롤백

**증상**:
```
경고: 트랜잭션 롤백 - 에러: duplicate key violates unique constraint
```

**해결 방법**:

1. 테이블 자르기:
```bash
python multi_db_load_tester_jdbc.py \
  --truncate \
  ...
```

2. 기본 키 시퀀스 재설정:

PostgreSQL:
```sql
TRUNCATE TABLE test_table RESTART IDENTITY CASCADE;
```

Oracle:
```sql
DROP SEQUENCE test_table_seq;
CREATE SEQUENCE test_table_seq START WITH 1;
```

MySQL:
```sql
TRUNCATE TABLE test_table;
```

---

## 7. DB 재시작 복구 문제

### 7.1 데이터베이스 재시작 후 시스템 중단

**증상**:
- DB 재시작 후 테스트가 중단됨
- 무한 루프 또는 응답 없음

**해결 방법**:

v2.3 이상을 사용 중인지 확인:
```bash
python multi_db_load_tester_jdbc.py --version
```

자동 복구 확인:
- v2.3은 지수적 백오프를 통한 자동 재연결 구현
- DB 재시작 후 15-20초 내에 자동 복구

로그에서 재연결 시도 확인:
```
14:32:45 - INFO: 연결 실패 - 재시도 1/3 (백오프: 100ms)
14:32:45 - INFO: 연결 실패 - 재시도 2/3 (백오프: 200ms)
14:32:45 - INFO: 연결 성공
```

---

### 7.2 연결 풀이 복구되지 않음

**증상**:
- DB 재시작 후 풀이 0/0으로 표시됨

**해결 방법**:

1. 풀 헬스 체크 확인:
```bash
python multi_db_load_tester_jdbc.py \
  --idle-check-interval 30 \
  --keepalive-time 30 \
  ...
```

2. 최대 수명 감소:
```bash
python multi_db_load_tester_jdbc.py \
  --max-lifetime 900 \
  ...
```

3. 로그에서 풀 재구축 확인:
```
14:33:00 - INFO: 연결 풀 재구축 중...
14:33:05 - INFO: 풀 상태: 100/200 (활성/총)
```

---

## 8. 로그 및 디버깅

### 8.1 상세 로깅 활성화

**해결 방법**:
```bash
python multi_db_load_tester_jdbc.py \
  --log-level DEBUG \
  ...
```

### 8.2 로그 파일 확인

**로그 파일 위치**:
```
multi_db_load_test_jdbc.log       (INFO 및 이상)
multi_db_load_test_jdbc_error.log (WARNING 및 이상)
```

### 8.3 JPype 디버깅

**해결 방법**:
```python
import jpype
import jpype.imports

# JPype 디버그 모드
jpype.startJVM(classpath=[...], convertStrings=False)
```

---

## 9. 일반적인 오류 메시지

### 9.1 Python 오류

| 오류 | 원인 | 해결 방법 |
|------|-------------|------------------|
| `ModuleNotFoundError` | 패키지 미설치 | `pip install JPype1 jaydebeapi` |
| `ImportError` | 잘못된 Python 버전 | Python 3.10+ 사용 |
| `TypeError` | 잘못된 인수 타입 | 인수 값 확인 |
| `KeyboardInterrupt` | 사용자 중단 (Ctrl+C) | 정상 - 우아한 종료 |

### 9.2 데이터베이스 오류

| 오류 | 원인 | 해결 방법 |
|------|-------------|------------------|
| `Connection refused` | DB 서버 중지 | DB 서버 시작 |
| `Authentication failed` | 잘못된 자격 증명 | 사용자/비밀번호 확인 |
| `Timeout` | 네트워크 지연 | 타임아웃 증가 |
| `Table not found` | 스키마 미설정 | `--skip-schema-setup` 제거 |
| `Permission denied` | 권한 부족 | 사용자 권한 부여 |

---

## 10. 모범 사례

### 10.1 테스트 전

1. **데이터베이스 상태 확인**:
   - DB 서버가 실행 중인지 확인
   - 충분한 리소스가 있는지 확인
   - 사용자 권한 확인

2. **구성 검증**:
   - 연결 정보 확인
   - 풀 크기 최적화
   - 적절한 워밍업 기간 설정

3. **테스트 환경 준비**:
   - 충분한 디스크 공간 확인
   - 네트워크 대역폭 확인
   - 다른 부하 프로세스 중지

### 10.2 테스트 중

1. **모니터링**:
   - 실시간 TPS 및 레이턴시 모니터링
   - 데이터베이스 리소스 모니터링
   - 에러 로그 확인

2. **조정**:
   - 필요한 경우 스레드 수 조정
   - 배치 크기 최적화
   - 속도 제한 조정

### 10.3 테스트 후

1. **결과 분석**:
   - TPS 및 레이턴시 분석
   - 에러 패턴 확인
   - 풀 통계 검토

2. **정리**:
   - 불필요한 연결 닫기
   - 임시 파일 삭제
   - 로그 보관

---

## 11. 지원 및 추가 리소스

### 11.1 로그 수집

문제 보고 시 다음 정보 포함:
1. Python 버전: `python --version`
2. Java 버전: `java -version`
3. 전체 명령줄 인수
4. 로그 파일 (main 및 error)
5. 데이터베이스 버전
6. 시스템 사양 (OS, CPU, RAM)

### 11.2 유용한 링크

- **프로젝트 저장소**: GitHub
- **이슈 추적**: GitHub Issues
- **문서**: README.md, TRD.md, API-Documentation.md

---

**문서 종료**
