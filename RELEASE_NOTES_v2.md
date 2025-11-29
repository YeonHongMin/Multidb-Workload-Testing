# Release Notes v2.0 - SQL Server & Tibero 지원 추가

## 🎉 주요 변경사항

### 신규 지원 데이터베이스 (2개 추가)

#### 1. SQL Server 2016+ 지원
- **드라이버**: pyodbc
- **PK 생성**: IDENTITY (자동 증가)
- **파티셔닝**: PARTITION FUNCTION + PARTITION SCHEME (16개 파티션)
- **특징**:
  - OUTPUT 절로 삽입된 ID 즉시 반환
  - Queue 기반 커넥션 풀 구현
  - ODBC Driver 17 for SQL Server 필요
  - Windows, Linux, macOS 모두 지원

#### 2. Tibero 6+ 지원
- **드라이버**: oracledb (Oracle 호환)
- **PK 생성**: SEQUENCE.NEXTVAL
- **파티셔닝**: HASH PARTITION (16개)
- **특징**:
  - Oracle과 동일한 문법 사용
  - Oracle 드라이버로 호환
  - 국산 DBMS 지원
  - 금융권/공공기관 적합

---

## 📊 지원 데이터베이스 비교표

| 데이터베이스 | 버전 | 드라이버 | PK 생성 | 파티셔닝 | 풀 제한 | 포트 |
|-------------|------|---------|---------|----------|---------|------|
| Oracle | 19c+ | oracledb | SEQUENCE | HASH | 무제한 | 1521 |
| PostgreSQL | 11+ | psycopg2 | BIGSERIAL | HASH | 무제한 | 5432 |
| MySQL | 5.7+ | mysql-connector | AUTO_INCREMENT | HASH | 32/풀 | 3306 |
| **SQL Server** | **2016+** | **pyodbc** | **IDENTITY** | **RANGE** | **무제한** | **1433** |
| **Tibero** | **6+** | **oracledb** | **SEQUENCE** | **HASH** | **무제한** | **8629** |

---

## 📁 새로운 파일 목록

### v2.0 신규 파일

#### 핵심 프로그램
- **`multi_db_load_tester_v2.py`** (43KB)
  - 5개 데이터베이스 지원
  - SQL Server 및 Tibero 어댑터 추가
  - Queue 기반 커넥션 풀 구현

#### 의존성
- **`requirements_v2.txt`**
  - pyodbc 추가 (SQL Server용)
  - 기존 드라이버 유지

#### 문서
- **`README_v2.md`** (12KB)
  - SQL Server 및 Tibero 설치/사용 가이드
  - 데이터베이스별 모니터링 쿼리
  - 성능 튜닝 팁

- **`QUICKSTART_v2.md`** (6.6KB)
  - 5분 빠른 시작 가이드
  - 데이터베이스별 특징 요약

#### DDL 스크립트
- **`sqlserver_schema.sql`** (4.5KB)
  - PARTITION FUNCTION 생성
  - PARTITION SCHEME 생성
  - 16개 파티션 테이블 생성

- **`tibero_schema.sql`** (4.1KB)
  - Oracle 호환 DDL
  - SEQUENCE + HASH PARTITION
  - Tibero 전용 주석

#### 실행 스크립트
- **`run_sqlserver_test.sh`** (976B)
  - SQL Server 부하 테스트 실행
  - ODBC 드라이버 안내

- **`run_tibero_test.sh`** (907B)
  - Tibero 부하 테스트 실행
  - 포트 8629 사용

---

## 🚀 빠른 시작

### SQL Server

```bash
# 1. ODBC 드라이버 설치 (Windows/Linux)
# Windows: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
# Linux: sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17

# 2. 스키마 생성
sqlcmd -S localhost -d testdb -U sa -P password -i sqlserver_schema.sql

# 3. 부하 테스트
python multi_db_load_tester_v2.py \
    --db-type sqlserver \
    --host localhost \
    --database testdb \
    --user sa \
    --password your_password \
    --thread-count 200 \
    --test-duration 300
```

### Tibero

```bash
# 1. Tibero 리스너 시작
tbdown -l && tbboot -l

# 2. 스키마 생성
tbsql user/pass@tibero @tibero_schema.sql

# 3. 부하 테스트
python multi_db_load_tester_v2.py \
    --db-type tibero \
    --host "localhost:8629/tibero" \
    --user test_user \
    --password test_pass \
    --thread-count 200 \
    --test-duration 300
```

---

## 🔧 기술적 개선사항

### SQL Server 어댑터
```python
class SQLServerAdapter(DatabaseAdapter):
    """SQL Server 데이터베이스 어댑터"""
    
    # Queue 기반 커넥션 풀 구현
    def create_connection_pool(self, config):
        self.connection_queue = queue.Queue(maxsize=config.max_pool_size)
        # 초기 커넥션 생성...
    
    # OUTPUT 절로 ID 반환
    def execute_insert(self, cursor, thread_id, random_data):
        sql = """
        INSERT INTO load_test (thread_id, value_col, random_data)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?)
        """
        # ...
```

### Tibero 어댑터
```python
class TiberoAdapter(DatabaseAdapter):
    """Tibero 데이터베이스 어댑터 (Oracle 호환)"""
    
    # Oracle 드라이버 사용
    def create_connection_pool(self, config):
        self.pool = oracledb.create_pool(
            user=config.user,
            password=config.password,
            dsn=config.host,
            # ...
        )
```

---

## 📊 성능 벤치마크 (예시)

### 테스트 환경
- CPU: Intel Xeon 8 cores
- Memory: 32GB
- 스레드: 200
- 테스트 시간: 300초

### 결과 (TPS)

| 데이터베이스 | TPS | 비고 |
|-------------|-----|------|
| Oracle 19c | 2,500 | SEQUENCE 캐시 1000 |
| PostgreSQL 14 | 2,300 | 기본 설정 |
| MySQL 8.0 | 1,800 | InnoDB 버퍼 풀 8GB |
| **SQL Server 2019** | **2,100** | **기본 설정** |
| **Tibero 7** | **2,400** | **SEQUENCE 캐시 1000** |

*실제 성능은 하드웨어, 네트워크, DB 설정에 따라 다를 수 있습니다.*

---

## 🐛 알려진 이슈 및 제한사항

### SQL Server
1. **파티셔닝**: HASH 파티션이 아닌 RANGE 파티션으로 구현
   - 해결: 값 범위를 균등하게 분산하여 HASH 효과 흉내
   
2. **ODBC 드라이버 필수**: 별도 설치 필요
   - 해결: README_v2.md의 설치 가이드 참조

### Tibero
1. **버전 호환성**: Tibero 6와 7에서 일부 차이 있음
   - 해결: 공식 문서 확인 및 버전별 테스트 권장

2. **드라이버**: Oracle 드라이버 사용
   - 참고: oracledb 1.x는 Tibero 6, 2.x는 Tibero 7 권장

---

## 🔄 마이그레이션 가이드

### v1.0에서 v2.0으로

#### 변경 없음 (하위 호환)
- v1.0의 Oracle, PostgreSQL, MySQL 기능 모두 유지
- 기존 스크립트 그대로 사용 가능

#### 새로운 기능 사용
```bash
# v1.0 파일 유지 (선택)
multi_db_load_tester.py  # 3개 DB 지원
requirements.txt

# v2.0 파일 추가
multi_db_load_tester_v2.py  # 5개 DB 지원
requirements_v2.txt
```

---

## 📚 추가 리소스

### 공식 문서
- [SQL Server ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/)
- [Tibero 공식 사이트](https://technet.tmaxsoft.com/ko/front/main/main.do)
- [pyodbc 문서](https://github.com/mkleehammer/pyodbc/wiki)

### 튜토리얼
- `README_v2.md` - 전체 문서
- `QUICKSTART_v2.md` - 빠른 시작
- 실행 스크립트 (`run_*.sh`) - 설정 예시

---

## 🎯 로드맵

### v2.1 (계획)
- [ ] MS SQL Server Always On 지원
- [ ] Tibero TAC (Cluster) 지원
- [ ] 성능 리포트 자동 생성
- [ ] Grafana 대시보드 통합

### v3.0 (계획)
- [ ] MongoDB 지원
- [ ] Cassandra 지원
- [ ] 분산 부하 테스트 (여러 클라이언트)
- [ ] 웹 UI 추가

---

## 👥 기여자

- Jeremiah - 초기 개발 및 v2.0 릴리즈

## 📄 라이선스

MIT License

---

## 🙏 감사의 말

SQL Server와 Tibero 지원을 추가하여 더 많은 데이터베이스 환경에서 부하 테스트를 수행할 수 있게 되었습니다. 

특히:
- **엔터프라이즈**: SQL Server를 사용하는 기업
- **금융/공공**: Tibero를 사용하는 금융권 및 공공기관
- **다양한 환경**: 여러 DBMS를 동시에 평가하는 조직

모두 이 도구의 혜택을 받으실 수 있습니다.

---

**Release Date**: 2025-01-15  
**Version**: 2.0.0  
**Author**: Jeremiah

🚀 **Happy Testing with 5 Databases!**
