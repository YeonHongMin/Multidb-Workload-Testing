# JDBC 드라이버 버전으로 전환 완료

## 📋 변경 사항 요약

기존 Python 네이티브 드라이버 방식에서 **./jre 디렉터리의 JDBC 드라이버**를 사용하는 방식으로 전환했습니다.

## 🎯 주요 변경점

### 1. 드라이버 방식 변경

**이전 (Python Native):**
```python
import oracledb          # Oracle
import psycopg2          # PostgreSQL
import mysql.connector   # MySQL
import pyodbc            # SQL Server
```

**현재 (JDBC):**
```python
import jaydebeapi        # 모든 데이터베이스 통합
# ./jre 디렉터리의 JAR 파일 사용
```

### 2. 커넥션 풀 구현

**이전:** 각 드라이버별 네이티브 풀
```python
oracledb.create_pool()           # Oracle
pg_pool.ThreadedConnectionPool() # PostgreSQL
mysql_pooling.MySQLConnectionPool() # MySQL
```

**현재:** 커스텀 Queue 기반 통합 풀
```python
class JDBCConnectionPool:
    """모든 JDBC 드라이버에서 동작하는 통합 풀"""
    def __init__(self, jdbc_url, driver_class, jar_file, ...):
        self.pool = queue.Queue(maxsize=max_size)
        # ...
```

### 3. JDBC 드라이버 자동 탐색

```python
def find_jdbc_jar(db_type: str, jre_dir: str = './jre'):
    """./jre 디렉터리에서 JDBC JAR 파일 자동 탐색"""
    driver_info = JDBC_DRIVERS[db_type]
    pattern = os.path.join(jre_dir, '**', driver_info.jar_pattern)
    jar_files = glob.glob(pattern, recursive=True)
    return sorted(jar_files)[-1]  # 최신 버전 사용
```

### 4. 데이터베이스별 JDBC URL 생성

```python
JDBC_DRIVERS = {
    'oracle': JDBCDriverInfo(
        driver_class='oracle.jdbc.OracleDriver',
        jar_pattern='ojdbc*.jar',
        url_template='jdbc:oracle:thin:@{host}:{port}:{sid}'
    ),
    'postgresql': JDBCDriverInfo(
        driver_class='org.postgresql.Driver',
        jar_pattern='postgresql-*.jar',
        url_template='jdbc:postgresql://{host}:{port}/{database}'
    ),
    # ... MySQL, SQL Server, Tibero
}
```

## 📁 생성된 파일 목록

### 1. 메인 프로그램
- **multi_db_load_tester_jdbc.py** (46KB)
  - JDBC 드라이버 사용
  - 5개 데이터베이스 지원
  - 통합 커넥션 풀
  - 자동 드라이버 탐색

### 2. 의존성 파일
- **requirements_jdbc.txt**
  ```
  jaydebeapi>=1.2.3
  JPype1>=1.4.1
  python-dotenv>=1.0.0
  ```

### 3. 문서
- **README_JDBC.md** (11KB)
  - 전체 사용 가이드
  - JDBC 드라이버 다운로드 위치
  - 실행 예제
  - 문제 해결 가이드

- **INSTALL_JDBC.md** (6.8KB)
  - 단계별 설치 가이드
  - Java 환경 설정
  - JDBC 드라이버 배치 방법
  - 문제 해결

### 4. 실행 스크립트
- **run_oracle_jdbc_test.sh** (1.2KB)
- **run_postgresql_jdbc_test.sh** (1.1KB)

## 🗂️ 디렉터리 구조

```
project/
├── multi_db_load_tester_jdbc.py    # 메인 프로그램
├── requirements_jdbc.txt            # Python 패키지
├── README_JDBC.md                   # 사용 가이드
├── INSTALL_JDBC.md                  # 설치 가이드
├── run_oracle_jdbc_test.sh          # Oracle 실행 스크립트
├── run_postgresql_jdbc_test.sh      # PostgreSQL 실행 스크립트
└── jre/                             # JDBC 드라이버 디렉터리
    ├── oracle/
    │   └── ojdbc8.jar              # Oracle JDBC 드라이버
    ├── tibero/
    │   └── tibero6-jdbc.jar        # Tibero JDBC 드라이버
    ├── postgresql/
    │   └── postgresql-42.7.3.jar   # PostgreSQL JDBC 드라이버
    ├── mysql/
    │   └── mysql-connector-java-8.0.33.jar  # MySQL JDBC 드라이버
    └── sqlserver/
        └── mssql-jdbc-12.6.1.jre11.jar      # SQL Server JDBC 드라이버
```

## 🚀 빠른 시작

### 1단계: 환경 준비

```bash
# Python 패키지 설치
pip install -r requirements_jdbc.txt

# Java 확인 (JRE 8+ 필요)
java -version
```

### 2단계: JDBC 드라이버 배치

```bash
# 디렉터리 생성
mkdir -p jre/{oracle,tibero,postgresql,mysql,sqlserver}

# 각 드라이버를 해당 디렉터리에 배치
# 예: cp ~/Downloads/ojdbc8.jar ./jre/oracle/
```

### 3단계: 데이터베이스 스키마 생성

```bash
# DDL 출력
python multi_db_load_tester_jdbc.py --db-type oracle --print-ddl

# 데이터베이스에서 실행
sqlplus user/pass@host:1521/sid @oracle_schema.sql
```

### 4단계: 부하 테스트 실행

```bash
# Oracle 예제
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host localhost \
    --port 1521 \
    --sid XEPDB1 \
    --user test_user \
    --password password \
    --thread-count 200 \
    --test-duration 300

# 또는 스크립트 사용
chmod +x run_oracle_jdbc_test.sh
./run_oracle_jdbc_test.sh
```

## 🔄 Python Native vs JDBC 비교

| 항목 | Python Native | JDBC |
|------|---------------|------|
| **설치** | pip install만으로 간단 | JAR 파일 수동 배치 필요 |
| **성능** | 더 빠름 | 약간 느림 (JNI 오버헤드) |
| **호환성** | DB별 드라이버 필요 | 모든 JDBC 드라이버 지원 |
| **안정성** | 드라이버 의존적 | 매우 안정적 (Java 표준) |
| **기업 환경** | 환경에 따라 다름 | 선호됨 (표준화) |
| **유지보수** | Python 버전 의존성 | Java만 있으면 됨 |

## ⚙️ 주요 기능

### 1. 자동 JDBC 드라이버 탐색

```python
# ./jre 디렉터리에서 자동으로 최신 버전 찾기
jar_file = find_jdbc_jar('oracle', './jre')
# Found: ./jre/oracle/ojdbc8.jar
```

### 2. 통합 커넥션 풀

```python
# 모든 DB에 동일한 풀 인터페이스
pool = JDBCConnectionPool(
    jdbc_url=jdbc_url,
    driver_class=driver_class,
    jar_file=jar_file,
    user=user,
    password=password,
    min_size=100,
    max_size=200
)
```

### 3. 에러 복구

```python
# 연속 5회 에러 시 커넥션 재생성
if consecutive_errors >= 5:
    db_adapter.release_connection(connection, is_error=True)
    connection = None
    perf_counter.increment_connection_recreate()
```

### 4. 실시간 모니터링

```
[Monitor] Stats - Inserts: 12,450 | Selects: 12,450 | Errors: 0 | 
Ver.Fail: 0 | Conn.Recreate: 0 | Avg TPS: 2490.00 | 
Interval TPS: 2490.00 | Elapsed: 5.0s
```

## 📊 지원 데이터베이스

| 데이터베이스 | JDBC 드라이버 | 기본 포트 | JDBC URL 형식 |
|-------------|--------------|----------|--------------|
| **Oracle** | ojdbc8.jar | 1521 | `jdbc:oracle:thin:@host:port:sid` |
| **Tibero** | tibero6-jdbc.jar | 8629 | `jdbc:tibero:thin:@host:port:sid` |
| **PostgreSQL** | postgresql-42.x.x.jar | 5432 | `jdbc:postgresql://host:port/database` |
| **MySQL** | mysql-connector-java-8.x.x.jar | 3306 | `jdbc:mysql://host:port/database` |
| **SQL Server** | mssql-jdbc-12.x.x.jar | 1433 | `jdbc:sqlserver://host:port;databaseName=db` |

## 🔧 필수 요구사항

### 소프트웨어
- Python 3.10 이상
- Java Runtime Environment (JRE) 8 이상
- JDBC 드라이버 JAR 파일들

### 하드웨어 (권장)
- CPU: 8 코어 이상
- 메모리: 16GB 이상
- 네트워크: 1Gbps 이상

## 📝 사용 예제

### Oracle

```bash
python multi_db_load_tester_jdbc.py \
    --db-type oracle \
    --host db-server \
    --port 1521 \
    --sid ORCL \
    --user test_user \
    --password password123 \
    --thread-count 200 \
    --test-duration 300 \
    --min-pool-size 200 \
    --max-pool-size 300 \
    --jre-dir ./jre
```

### PostgreSQL

```bash
python multi_db_load_tester_jdbc.py \
    --db-type postgresql \
    --host localhost \
    --port 5432 \
    --database testdb \
    --user test_user \
    --password password123 \
    --thread-count 200 \
    --test-duration 300
```

### Tibero

```bash
python multi_db_load_tester_jdbc.py \
    --db-type tibero \
    --host tibero-server \
    --port 8629 \
    --sid tibero \
    --user test_user \
    --password password123 \
    --thread-count 200 \
    --test-duration 300
```

## 🐛 문제 해결

### JDBC 드라이버를 찾을 수 없음

```bash
# 드라이버 확인
ls -R jre/

# 드라이버 배치
cp ~/Downloads/ojdbc8.jar ./jre/oracle/
```

### Java 관련 에러

```bash
# Java 설치 확인
java -version

# JAVA_HOME 설정
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

### 메모리 부족

```python
# JVM 힙 메모리 증가
import jpype
jpype.startJVM(jpype.getDefaultJVMPath(), "-Xmx4g")  # 4GB
```

## 📚 추가 리소스

- **README_JDBC.md**: 전체 사용 가이드
- **INSTALL_JDBC.md**: 상세 설치 가이드
- **로그 파일**: multi_db_load_test_jdbc.log

## ✅ 체크리스트

설치 및 실행 전 확인사항:

- [ ] Python 3.10+ 설치
- [ ] Java (JRE/JDK) 8+ 설치
- [ ] `pip install -r requirements_jdbc.txt` 완료
- [ ] JDBC 드라이버 JAR 파일을 `./jre` 에 배치
- [ ] 데이터베이스 스키마 생성 완료
- [ ] 연결 정보 확인 (host, port, user, password)

## 🎉 완료!

JDBC 드라이버를 사용하는 멀티 데이터베이스 부하 테스트 프로그램이 준비되었습니다.

**다음 단계:**
1. INSTALL_JDBC.md를 참조하여 JDBC 드라이버 설치
2. 데이터베이스 스키마 생성
3. 부하 테스트 실행

**지원이 필요하면:**
- README_JDBC.md의 "문제 해결" 섹션 참조
- 로그 레벨을 DEBUG로 설정하여 상세 정보 확인
- multi_db_load_test_jdbc.log 파일 확인
