# JDBC 버전 설치 가이드

## 1단계: Python 환경 준비

```bash
# Python 버전 확인 (3.10 이상 필요)
python3 --version

# 가상환경 생성
python3 -m venv venv

# 가상환경 활성화
# Linux/Mac:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# 패키지 설치
pip install -r requirements_jdbc.txt
```

## 2단계: Java 환경 확인

```bash
# Java 버전 확인 (JRE 8 이상 필요)
java -version

# JAVA_HOME 환경 변수 설정 (필요시)
# Linux/Mac:
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Windows:
set JAVA_HOME=C:\Program Files\Java\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%
```

### Java 설치 (설치되지 않은 경우)

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-11-jre
```

**CentOS/RHEL:**
```bash
sudo yum install java-11-openjdk
```

**macOS:**
```bash
brew install openjdk@11
```

**Windows:**
- https://www.oracle.com/java/technologies/downloads/ 에서 다운로드

## 3단계: JDBC 드라이버 다운로드 및 배치

### 디렉터리 구조 생성

```bash
mkdir -p jre/oracle
mkdir -p jre/tibero
mkdir -p jre/postgresql
mkdir -p jre/mysql
mkdir -p jre/sqlserver
```

### Oracle JDBC 드라이버

**다운로드:**
1. https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html 방문
2. Oracle Database 버전에 맞는 JDBC 드라이버 선택
   - Oracle 11g/12c → ojdbc8.jar (JDK 8)
   - Oracle 12c+ → ojdbc8.jar 또는 ojdbc11.jar (JDK 11+)

**배치:**
```bash
# 다운로드한 파일을 jre/oracle 디렉터리로 이동
cp ~/Downloads/ojdbc8.jar ./jre/oracle/
```

**확인:**
```bash
ls -l ./jre/oracle/ojdbc8.jar
```

### Tibero JDBC 드라이버

**다운로드:**
1. Tibero 설치 디렉터리의 `client/lib` 찾기
2. `tibero6-jdbc.jar` 또는 `tibero7-jdbc.jar` 복사

**배치:**
```bash
# Tibero 설치 경로에서 복사
cp $TB_HOME/client/lib/tibero6-jdbc.jar ./jre/tibero/
```

**확인:**
```bash
ls -l ./jre/tibero/tibero6-jdbc.jar
```

### PostgreSQL JDBC 드라이버

**다운로드:**
1. https://jdbc.postgresql.org/download/ 방문
2. 최신 버전 다운로드 (예: postgresql-42.7.3.jar)

**배치:**
```bash
cp ~/Downloads/postgresql-42.7.3.jar ./jre/postgresql/
```

**확인:**
```bash
ls -l ./jre/postgresql/postgresql-*.jar
```

### MySQL JDBC 드라이버

**다운로드:**
1. https://dev.mysql.com/downloads/connector/j/ 방문
2. "Platform Independent" 선택
3. ZIP 파일 다운로드 및 압축 해제
4. `mysql-connector-java-8.x.x.jar` 파일 찾기

**배치:**
```bash
cp ~/Downloads/mysql-connector-java-8.0.33/mysql-connector-java-8.0.33.jar ./jre/mysql/
```

**확인:**
```bash
ls -l ./jre/mysql/mysql-connector-*.jar
```

### SQL Server JDBC 드라이버

**다운로드:**
1. https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server 방문
2. 최신 버전 다운로드
3. 압축 해제 후 `mssql-jdbc-xx.x.x.jre11.jar` 파일 찾기

**배치:**
```bash
cp ~/Downloads/sqljdbc_12.6/enu/jars/mssql-jdbc-12.6.1.jre11.jar ./jre/sqlserver/
```

**확인:**
```bash
ls -l ./jre/sqlserver/mssql-jdbc-*.jar
```

## 4단계: 드라이버 확인

모든 JDBC 드라이버가 올바르게 배치되었는지 확인:

```bash
tree jre/
# 또는
find jre/ -name "*.jar"
```

**예상 출력:**
```
jre/
├── oracle
│   └── ojdbc8.jar
├── tibero
│   └── tibero6-jdbc.jar
├── postgresql
│   └── postgresql-42.7.3.jar
├── mysql
│   └── mysql-connector-java-8.0.33.jar
└── sqlserver
    └── mssql-jdbc-12.6.1.jre11.jar
```

## 5단계: 연결 테스트

각 데이터베이스별로 간단한 연결 테스트:

### Oracle
```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost \
    --port 1521 \
    --sid XEPDB1 \
    --user test_user \
    --password password \
    --thread-count 1 \
    --test-duration 5
```

### PostgreSQL
```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost \
    --port 5432 \
    --database testdb \
    --user test_user \
    --password password \
    --thread-count 1 \
    --test-duration 5
```

### MySQL
```bash
python multi_db_load_tester_jdbc.py \
    --db-type mysql \
    --host localhost \
    --port 3306 \
    --database testdb \
    --user test_user \
    --password password \
    --thread-count 1 \
    --test-duration 5
```

## 6단계: 데이터베이스 스키마 생성

각 데이터베이스에 테스트 테이블 생성:

### DDL 출력
```bash
# Oracle DDL
python multi_db_load_tester_jdbc.py --db-type oracle --print-ddl > oracle_schema.sql

# PostgreSQL DDL
python multi_db_load_tester_jdbc.py --db-type postgresql --print-ddl > postgresql_schema.sql

# MySQL DDL
python multi_db_load_tester_jdbc.py --db-type mysql --print-ddl > mysql_schema.sql
```

### DDL 실행

**Oracle:**
```bash
sqlplus test_user/password@localhost:1521/XEPDB1 @oracle_schema.sql
```

**PostgreSQL:**
```bash
psql -U test_user -d testdb -f postgresql_schema.sql
```

**MySQL:**
```bash
mysql -u test_user -p testdb < mysql_schema.sql
```

## 7단계: 전체 부하 테스트 실행

```bash
# Oracle
./run_oracle_jdbc_test.sh

# PostgreSQL
./run_postgresql_jdbc_test.sh
```

## 문제 해결

### "JDBC driver not found" 에러

**문제:** JAR 파일을 찾을 수 없음

**해결:**
1. JAR 파일이 올바른 디렉터리에 있는지 확인
2. 파일명이 패턴과 일치하는지 확인
   - Oracle: ojdbc*.jar
   - PostgreSQL: postgresql-*.jar
   - MySQL: mysql-connector-*.jar

### "No JVM shared library file found" 에러

**문제:** Java가 설치되지 않았거나 JAVA_HOME이 설정되지 않음

**해결:**
```bash
# Java 설치 확인
java -version

# JAVA_HOME 설정 (Linux/Mac)
export JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 | grep 'java.home' | awk '{print $3}')

# JAVA_HOME 설정 (Windows)
# 시스템 환경 변수에서 JAVA_HOME을 JDK 설치 경로로 설정
```

### JPype 설치 에러

**문제:** JPype1 설치 실패

**해결:**
```bash
# 먼저 시스템 의존성 설치 (Linux)
sudo apt-get install python3-dev

# JPype1 재설치
pip uninstall JPype1
pip install JPype1
```

### 메모리 부족 에러

**문제:** Java heap space 에러

**해결:** JVM 힙 메모리 증가

```python
# multi_db_load_tester_jdbc.py 파일 상단 수정
import jpype
import jaydebeapi

# JVM 시작 (프로그램 시작 시 한 번만)
if not jpype.isJVMStarted():
    jpype.startJVM(jpype.getDefaultJVMPath(), "-Xmx4g")  # 4GB 힙 메모리
```

## 완료

설치가 완료되었습니다! 이제 부하 테스트를 실행할 수 있습니다.

```bash
# 전체 테스트 실행
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost \
    --sid XEPDB1 \
    --user test_user \
    --password password \
    --thread-count 200 \
    --test-duration 300
```
