# 프로젝트 구조 (Project Structure)

```
multi-db-load-tester/
│
├── multi_db_load_tester.py    # 메인 프로그램 (Oracle, PostgreSQL, MySQL 지원)
├── requirements.txt            # Python 패키지 의존성
│
├── README.md                   # 전체 문서 (설치, 사용법, 튜닝 팁)
├── QUICKSTART.md              # 5분 빠른 시작 가이드
├── PROJECT_STRUCTURE.md       # 이 파일
│
├── env.example                # 환경 변수 설정 예시
│
├── oracle_schema.sql          # Oracle DDL 스크립트
├── postgresql_schema.sql      # PostgreSQL DDL 스크립트
├── mysql_schema.sql           # MySQL DDL 스크립트
│
├── run_oracle_test.sh         # Oracle 부하 테스트 실행 스크립트
├── run_postgresql_test.sh     # PostgreSQL 부하 테스트 실행 스크립트
└── run_mysql_test.sh          # MySQL 부하 테스트 실행 스크립트
```

## 파일 설명

### 핵심 파일

#### `multi_db_load_tester.py`
- **역할**: 메인 프로그램
- **크기**: ~34KB
- **기능**:
  - Oracle, PostgreSQL, MySQL 지원
  - 멀티스레드 부하 테스트
  - 커넥션 풀링 관리
  - 실시간 성능 모니터링
  - 자동 에러 복구
- **의존성**: oracledb, psycopg2-binary, mysql-connector-python

#### `requirements.txt`
- **역할**: Python 패키지 의존성 정의
- **내용**:
  ```
  oracledb>=2.0.0
  psycopg2-binary>=2.9.0
  mysql-connector-python>=8.0.0
  python-dotenv>=1.0.0
  ```

### 문서 파일

#### `README.md`
- **크기**: ~16KB
- **내용**:
  - 전체 프로젝트 개요
  - 상세 설치 가이드
  - 모든 DB의 DDL 스크립트
  - 사용 방법 및 예시
  - 성능 튜닝 팁
  - 문제 해결 가이드
  - 데이터베이스별 모니터링 쿼리

#### `QUICKSTART.md`
- **크기**: ~4.5KB
- **내용**:
  - 5분 안에 시작하기
  - 최소한의 단계로 테스트 실행
  - 빠른 참조용

#### `PROJECT_STRUCTURE.md`
- **역할**: 프로젝트 구조 설명 (이 파일)

### DDL 스크립트

#### `oracle_schema.sql`
- **크기**: ~3.4KB
- **내용**:
  - SEQUENCE 생성
  - 16개 파티션 HASH 테이블 생성
  - PRIMARY KEY 제약조건
  - 성능 최적화 인덱스
  - 확인 쿼리
- **실행**: `sqlplus username/password@service @oracle_schema.sql`

#### `postgresql_schema.sql`
- **크기**: ~4.8KB
- **내용**:
  - BIGSERIAL 사용 (자동 SEQUENCE)
  - 16개 HASH 파티션 테이블 생성 (PostgreSQL 11+)
  - 인덱스 생성
  - 확인 쿼리
- **실행**: `psql -U username -d database -f postgresql_schema.sql`

#### `mysql_schema.sql`
- **크기**: ~2.6KB
- **내용**:
  - AUTO_INCREMENT 사용
  - 16개 HASH 파티션 테이블 생성
  - 인덱스 생성
  - 확인 쿼리
- **실행**: `mysql -u username -p database < mysql_schema.sql`

### 실행 스크립트

#### `run_oracle_test.sh`
- **크기**: ~654B
- **용도**: Oracle 부하 테스트 빠른 실행
- **수정 필요**: DSN, 사용자명, 비밀번호
- **실행**: `./run_oracle_test.sh`

#### `run_postgresql_test.sh`
- **크기**: ~641B
- **용도**: PostgreSQL 부하 테스트 빠른 실행
- **수정 필요**: 호스트, 포트, 데이터베이스, 사용자명, 비밀번호
- **실행**: `./run_postgresql_test.sh`

#### `run_mysql_test.sh`
- **크기**: ~794B
- **용도**: MySQL 부하 테스트 빠른 실행
- **수정 필요**: 호스트, 포트, 데이터베이스, 사용자명, 비밀번호
- **실행**: `./run_mysql_test.sh`
- **참고**: MySQL은 풀당 최대 32개 커넥션 제한

### 설정 파일

#### `env.example`
- **크기**: ~1.4KB
- **용도**: 환경 변수 설정 템플릿
- **사용법**:
  1. `.env`로 복사: `cp env.example .env`
  2. 실제 값으로 수정
  3. 프로그램에서 자동 로드 (python-dotenv 사용 시)

## 코드 구조

### `multi_db_load_tester.py` 주요 클래스

```
multi_db_load_tester.py
│
├── PerformanceCounter          # 성능 통계 수집 (Thread-safe)
│
├── DatabaseAdapter (ABC)       # 데이터베이스 공통 인터페이스
│   ├── OracleAdapter          # Oracle 구현
│   ├── PostgreSQLAdapter      # PostgreSQL 구현
│   └── MySQLAdapter           # MySQL 구현
│
├── DatabaseConfig              # 데이터베이스 연결 설정
│
├── LoadTestWorker             # 개별 워커 스레드
│   ├── generate_random_data()
│   ├── execute_transaction()  # INSERT → COMMIT → SELECT → VERIFY
│   └── run()                  # 워커 메인 루프
│
├── MonitorThread              # 모니터링 스레드
│   └── run()                  # 5초마다 통계 출력
│
├── MultiDBLoadTester          # 메인 테스터 클래스
│   ├── _create_adapter()
│   ├── print_ddl()
│   ├── run_load_test()
│   └── _print_final_stats()
│
└── main()                     # 프로그램 엔트리포인트
```

## 실행 흐름

```
1. 명령행 인자 파싱
   └── DB 타입, 연결 정보, 테스트 파라미터

2. DatabaseAdapter 생성
   └── Oracle/PostgreSQL/MySQL 중 선택

3. 커넥션 풀 생성
   ├── 최소/최대 풀 크기 설정
   └── 드라이버별 최적화

4. 워커 스레드 시작 (N개)
   ├── 각 워커가 독립적으로 실행
   └── INSERT → COMMIT → SELECT → VERIFY 반복

5. 모니터링 스레드 시작
   └── 5초마다 통계 출력

6. 테스트 완료 대기
   └── duration 경과 또는 Ctrl+C

7. 최종 통계 출력
   └── TPS, 에러율, 성공률 등

8. 리소스 정리
   └── 커넥션 풀 종료
```

## 로그 파일

### `multi_db_load_test.log`
- **위치**: 프로그램 실행 디렉터리
- **내용**:
  - 모든 실행 로그
  - 에러 메시지 및 스택 트레이스
  - 성능 통계
  - 워커 스레드 상태
- **로그 레벨**: DEBUG, INFO, WARNING, ERROR
- **로테이션**: 수동 (필요시 삭제 또는 백업)

## 생성되는 데이터

### 데이터베이스 테이블: `load_test` (또는 `LOAD_TEST`)

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| id | NUMBER/BIGINT | PRIMARY KEY (SEQUENCE/AUTO_INCREMENT) |
| thread_id | VARCHAR | 워커 스레드 식별자 |
| value_col | VARCHAR | 테스트 값 |
| random_data | VARCHAR | 랜덤 데이터 (500자) |
| status | VARCHAR | 상태 (기본: 'ACTIVE') |
| created_at | TIMESTAMP | 생성 시각 |
| updated_at | TIMESTAMP | 수정 시각 |

### 데이터 특성
- **파티셔닝**: 16개 HASH 파티션 (id 기준)
- **인덱스**: thread_id, created_at 복합 인덱스
- **데이터 크기**: 행당 약 500-1000 바이트
- **예상 볼륨**: 
  - 100 TPS × 300초 = 30,000 rows (~30MB)
  - 1000 TPS × 600초 = 600,000 rows (~600MB)

## 시스템 요구사항

### 개발/테스트 환경
- **CPU**: 2 코어 이상
- **메모리**: 4GB 이상
- **디스크**: 1GB 여유 공간
- **Python**: 3.10 이상

### 프로덕션 부하 테스트 환경
- **CPU**: 8 코어 이상
- **메모리**: 16GB 이상
- **디스크**: 10GB 여유 공간
- **네트워크**: 1Gbps 이상
- **Python**: 3.10 이상

### 데이터베이스 서버
- **Oracle**: 19c 이상 (Enterprise Edition 권장)
- **PostgreSQL**: 11 이상 (파티셔닝 지원)
- **MySQL**: 5.7 이상 또는 8.0 이상 (InnoDB)

## 확장성

### 현재 지원
- 동시 스레드: 1 ~ 1000개
- 커넥션 풀: 100 ~ 1000개
- 테스트 시간: 1초 ~ 무제한
- 데이터베이스: Oracle, PostgreSQL, MySQL

### 향후 확장 가능
- 다른 데이터베이스 (MS SQL Server, MongoDB 등)
- 분산 부하 테스트 (여러 클라이언트)
- 웹 대시보드
- 자동 성능 보고서
- 커스텀 SQL 테스트

## 라이선스

MIT License

## 버전 히스토리

- **v1.0.0** (2025-01-15)
  - 초기 릴리스
  - Oracle, PostgreSQL, MySQL 지원
  - 멀티스레드 부하 테스트
  - 실시간 모니터링

---

더 자세한 정보는 `README.md`를 참조하세요.
