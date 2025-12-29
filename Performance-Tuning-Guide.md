# 성능 튜닝 가이드
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

이 문서는 멀티 데이터베이스 워크로드 테스트 도구의 성능을 최적화하는 방법을 설명합니다. 테스트 도구 자체의 튜닝과 데이터베이스 서버의 튜닝을 모두 다룹니다.

---

## 2. 테스트 도구 튜닝

### 2.1 연결 풀 최적화

#### 2.1.1 풀 크기 결정

**기본 원칙**:
- `min-pool-size` = `thread-count` × 0.5 ~ 0.7
- `max-pool-size` = `thread-count` × 1.0 ~ 1.5

**예시**:
```bash
# 100개 스레드
python multi_db_load_tester_jdbc.py \
  --thread-count 100 \
  --min-pool-size 50 \
  --max-pool-size 100 \
  ...
```

**성능 영향**:
- 너무 작은 풀 → 연결 대기 시간 증가
- 너무 큰 풀 → 데이터베이스 부하 증가, 메모리 사용량 증가

#### 2.1.2 풀 수명 관리

**max-lifetime 최적화**:
- 기본값: 1800초 (30분)
- 안정적인 연결: 3600초 (1시간) 증가
- 불안정한 연결: 900초 (15분) 감소

```bash
python multi_db_load_tester_jdbc.py \
  --max-lifetime 3600 \
  ...
```

**연결 누수 방지**:
```bash
python multi_db_load_tester_jdbc.py \
  --leak-detection-threshold 30 \
  --idle-check-interval 30 \
  ...
```

#### 2.1.3 유휴 연결 관리

**idle-timeout 및 keepalive-time 최적화**:
```bash
python multi_db_load_tester_jdbc.py \
  --idle-timeout 60 \
  --keepalive-time 30 \
  ...
```

**권장 설정**:
- `idle-timeout`: 30-60초
- `keepalive-time`: 30초

---

### 2.2 스레드 최적화

#### 2.2.1 적절한 스레드 수 결정

**CPU 코어 기반 계산**:
```
최적 스레드 수 = CPU 코어 × 2
높은 I/O 작업 = CPU 코어 × 4 ~ 8
```

**예시**:
```bash
# 4코어 CPU - 읽기/쓰기 혼합
python multi_db_load_tester_jdbc.py \
  --thread-count 200 \
  ...
```

**스레드 수 증가 테스트**:
1. 50개 스레드로 시작
2. 25개씩 점진적 증가
3. TPS가 더 이상 증가하지 않는 지점에서 중지
4. P99 레이턴시 모니터링

#### 2.2.2 램프업 사용

**점진적 스레드 활성화**:
```bash
python multi_db_load_tester_jdbc.py \
  --thread-count 200 \
  --ramp-up 60 \
  ...
```

**이점**:
- 데이터베이스에 갑작스러운 부하 방지
- 부하 점진적 증가로 병목 지점 식별
- 워밍업 효과 개선

---

### 2.3 배치 최적화

#### 2.3.1 배치 크기 결정

**배치 크기 가이드라인**:
| 작업 | 권장 배치 크기 | 최대 배치 크기 |
|------|----------------|----------------|
| INSERT | 50-200 | 1000 |
| UPDATE | 20-100 | 500 |
| DELETE | 20-100 | 500 |

**예시**:
```bash
# INSERT 성능 테스트
python multi_db_load_tester_jdbc.py \
  --mode insert-only \
  --batch-size 100 \
  ...
```

**배치 크기 테스트**:
1. 10개로 시작
2. 10개씩 점진적 증가
3. TPS/레이턴시 비교
4. 최적 지점 식별

#### 2.3.2 트랜잭션 관리

**단일 트랜잭션 per 배치**:
- 도구는 배치당 단일 트랜잭션 사용
- 자동 커밋 활성화
- 롤백 시 전체 배치 실패

**장점**:
- 네트워크 라운드트립 감소
- 트랜잭션 오버헤드 감소
- ACID 보장

---

### 2.4 속도 제한 최적화

#### 2.4.1 목표 TPS 설정

**TPS 제한 사용 사례**:
- 생산 환경 시뮬레이션
- 데이터베이스 용량 테스트
- 안정적인 부하 생성

```bash
python multi_db_load_tester_jdbc.py \
  --target-tps 1000 \
  ...
```

**토큰 버킷 파라미터**:
- 버스트 허용량: 목표 TPS × 2
- 최소 1초 동안 버스트 가능

#### 2.4.2 속도 제한 최적화

**안정적인 TPS 달성**:
```bash
python multi_db_load_tester_jdbc.py \
  --target-tps 800 \
  --thread-count 200 \
  --warmup 60 \
  ...
```

**TPS 튜닝**:
1. 높은 목표로 시작 (최대 용량 확인)
2. 안정성을 위해 20% 감소
3. P99 레이턴시 < 200ms 목표

---

### 2.5 워밍업 및 모니터링 최적화

#### 2.5.1 워밍업 기간

**권장 워밍업 기간**:
- 짧은 테스트 (< 5분): 30초
- 중간 테스트 (5-15분): 60초
- 장기 테스트 (> 15분): 120초

```bash
python multi_db_load_tester_jdbc.py \
  --warmup 60 \
  ...
```

**워밍업의 중요성**:
- JIT 컴파일 완료
- 연결 풀 안정화
- 데이터베이스 캐시 웜업

#### 2.5.2 모니터링 간격

**모니터링 간격 가이드라인**:
| 테스트 기간 | 모니터링 간격 |
|------------|--------------|
| < 5분 | 1.0초 |
| 5-30분 | 2.0-5.0초 |
| > 30분 | 5.0-10.0초 |

```bash
python multi_db_load_tester_jdbc.py \
  --monitor-interval 5.0 \
  ...
```

**서브초 윈도우**:
```bash
python multi_db_load_tester_jdbc.py \
  --sub-second-interval 200 \
  ...
```

---

### 2.6 메모리 최적화

#### 2.6.1 Python 힙 크기

**JPype 힙 크기 설정**:
```python
import jpype

# JVM 시작 전
jpype.startJVM(
    classpath=...,
    '-Xmx2g',  # 최대 힙 2GB
    '-Xms512m'  # 초기 힙 512MB
)
```

#### 2.6.2 메모리 사용량 최적화

**메모리 절약 팁**:
1. 스레드 수 감소
2. 배치 크기 감소
3. 모니터링 간격 증가
4. 로그 레벨 INFO로 설정

```bash
python multi_db_load_tester_jdbc.py \
  --thread-count 100 \
  --batch-size 50 \
  --monitor-interval 5.0 \
  --log-level INFO \
  ...
```

---

## 3. 데이터베이스별 최적화

### 3.1 Oracle 최적화

#### 3.1.1 파라미터 튜닝

```sql
-- 데이터베이스 버퍼 캐시 증가
ALTER SYSTEM SET db_cache_size = 4G SCOPE=SPFILE;

-- 공유 풀 크기 증가
ALTER SYSTEM SET shared_pool_size = 2G SCOPE=SPFILE;

-- 프로세스 및 세션 수 증가
ALTER SYSTEM SET processes = 500 SCOPE=SPFILE;
ALTER SYSTEM SET sessions = 600 SCOPE=SPFILE;

-- REDO 로그 크기 증가
ALTER SYSTEM SET log_buffer = 64M SCOPE=SPFILE;
```

#### 3.1.2 파티셔닝 활용

```sql
-- HASH 파티셔닝 (도구가 자동 생성)
CREATE TABLE test_table (
    id NUMBER PRIMARY KEY,
    thread_id VARCHAR2(50),
    ...
) PARTITION BY HASH (thread_id) PARTITIONS 16;
```

**이점**:
- 쿼리 병렬화
- 인덱스 크기 감소
- I/O 분산

#### 3.1.3 커서 공유

```sql
-- 커서 공유 활성화
ALTER SYSTEM SET cursor_sharing = FORCE SCOPE=SPFILE;
```

#### 3.1.4 네트워크 최적화

```bash
# Oracle JDBC 연결 속성
-Doracle.jdbc.defaultNChar=true
-Doracle.jdbc.convertNcharLobTypeToString=true
-Doracle.jdbc.TcpNoDelay=true
```

---

### 3.2 PostgreSQL 최적화

#### 3.2.1 postgresql.conf 튜닝

```ini
# 메모리 설정
shared_buffers = 4GB                    # RAM의 25%
effective_cache_size = 12GB              # RAM의 50-75%
work_mem = 64MB                         # per operation
maintenance_work_mem = 512MB
max_connections = 200

# WAL 설정
wal_buffers = 16MB
checkpoint_completion_target = 0.9
max_wal_size = 4GB
min_wal_size = 1GB

# 쿼리 플래너
random_page_cost = 1.1                  # SSD용
effective_io_concurrency = 200           # SSD용
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8

# 로깅
log_min_duration_statement = 1000        # 1초 이상 쿼리 로깅
```

#### 3.2.2 파티셔닝 활용

```sql
-- HASH 파티셔닝 (도구가 자동 생성)
CREATE TABLE test_table (
    id BIGSERIAL,
    thread_id VARCHAR(50),
    ...
) PARTITION BY HASH (thread_id);

CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 16, REMAINDER 0);
-- ... 나머지 파티션 생성
```

#### 3.2.3 인덱스 최적화

```sql
-- 인덱스 생성 전 분석
ANALYZE test_table;

-- CONCURRENTLY 옵션으로 잠금 없이 인덱스 생성
CREATE INDEX CONCURRENTLY idx_thread_id ON test_table(thread_id);
CREATE INDEX CONCURRENTLY idx_created_at ON test_table(created_at);

-- 부분 인덱스 (필요한 경우)
CREATE INDEX idx_active ON test_table(thread_id) WHERE status = 'ACTIVE';
```

#### 3.2.4 Autovacuum 튜닝

```ini
# postgresql.conf
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1s
autovacuum_vacuum_threshold = 5000
autovacuum_vacuum_scale_factor = 0.1
autovacuum_analyze_threshold = 5000
autovacuum_analyze_scale_factor = 0.05
```

---

### 3.3 MySQL 최적화

#### 3.3.1 my.cnf 튜닝

```ini
[mysqld]
# 연결 설정
max_connections = 500
max_connect_errors = 100000
connect_timeout = 10

# InnoDB 버퍼 풀
innodb_buffer_pool_size = 4G            # RAM의 50-70%
innodb_buffer_pool_instances = 4         # CPU 코어 수
innodb_log_file_size = 512M
innodb_log_buffer_size = 64M
innodb_flush_log_at_trx_commit = 2      # 성능 우선

# 쓰레드 설정
thread_cache_size = 100
thread_stack = 256K

# 쿼리 캐시 (MySQL 5.7 이하)
query_cache_size = 0                     # 비활성화 권장

# 임시 테이블
tmp_table_size = 256M
max_heap_table_size = 256M

# 로깅
slow_query_log = 1
long_query_time = 1
```

#### 3.3.2 파티셔닝 활용

```sql
-- HASH 파티셔닝 (도구가 자동 생성)
CREATE TABLE test_table (
    id BIGINT AUTO_INCREMENT,
    thread_id VARCHAR(50),
    ...
    PRIMARY KEY (id, thread_id)
) PARTITION BY HASH (thread_id) PARTITIONS 16;
```

#### 3.3.3 InnoDB 최적화

```sql
-- 버퍼 풀 상태 확인
SHOW STATUS LIKE 'Innodb_buffer_pool%';

-- 버퍼 풀 동적 크기 조정 (MySQL 5.7+)
SET GLOBAL innodb_buffer_pool_size = 4294967296; -- 4GB

-- 버퍼 풀 인스턴스 조정
SET GLOBAL innodb_buffer_pool_instances = 4;
```

#### 3.3.4 커넥터/J 최적화

```bash
# JDBC URL 파라미터
jdbc:mysql://host:3306/db?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&useLocalSessionState=true&rewriteBatchedStatements=true&cacheResultSetMetadata=true&cacheServerConfiguration=true&elideSetAutoCommits=true&maintainTimeStats=false
```

---

### 3.4 SQL Server 최적화

#### 3.4.1 파라미터 튜닝

```sql
-- 최대 메모리 설정 (RAM의 80%)
EXEC sp_configure 'max server memory', 65536; -- MB
RECONFIGURE;

-- 최소 메모리 설정 (RAM의 10%)
EXEC sp_configure 'min server memory', 8192; -- MB
RECONFIGURE;

-- 연결 수 증가
EXEC sp_configure 'user connections', 500;
RECONFIGURE;
```

#### 3.4.2 인덱스 최적화

```sql
-- 인덱스 생성
CREATE NONCLUSTERED INDEX idx_thread_id ON test_table(thread_id);
CREATE NONCLUSTERED INDEX idx_created_at ON test_table(created_at);

-- 인덱스 재구축
ALTER INDEX ALL ON test_table REBUILD;
```

#### 3.4.3 데이터베이스 설정

```sql
-- 복구 모드 (로그 증가 방지)
ALTER DATABASE testdb SET RECOVERY SIMPLE;

-- 자동 통계 업데이트
ALTER DATABASE testdb SET AUTO_UPDATE_STATISTICS ON;

-- 자동 통계 업데이트 비동기
ALTER DATABASE testdb SET AUTO_UPDATE_STATISTICS_ASYNC ON;
```

#### 3.4.4 TempDB 최적화

```sql
-- TempDB 파일 증가 (CPU 코어 수만큼)
ALTER DATABASE tempdb MODIFY FILE (NAME = tempdev, SIZE = 1GB, FILEGROWTH = 100MB);
```

---

### 3.5 Tibero 최적화

#### 3.5.1 파라미터 튜닝

```sql
-- 버퍼 풀 설정
ALTER SYSTEM SET MEMORY_TARGET = 4G;
ALTER SYSTEM SET MEMORY_MAX_TARGET = 4G;

-- 프로세스 수 증가
ALTER SYSTEM SET PROCESSES = 500;

-- 로그 버퍼 증가
ALTER SYSTEM SET LOG_BUFFER = 64M;
```

#### 3.5.2 파티셔닝 활용

```sql
-- HASH 파티셔닝 (도구가 자동 생성)
CREATE TABLE test_table (
    id NUMBER,
    thread_id VARCHAR2(50),
    ...
    PRIMARY KEY (id, thread_id)
) PARTITION BY HASH (thread_id) PARTITIONS 16;
```

---

### 3.6 IBM DB2 최적화

#### 3.6.1 파라미터 튜닝

```sql
-- 버퍼 풀 크기
UPDATE DB CFG FOR testdb USING BUFFPAGE SIZE 8192;

-- 최대 응용 프로그램 (MAXAPPLS)
UPDATE DB CFG FOR testdb USING MAXAPPLS 500;

-- 정렬 힙 (SHEAPTHRES)
UPDATE DB CFG FOR testdb USING SHEAPTHRES 10000;
```

#### 3.6.2 로그 크기

```sql
-- 로그 파일 크기
UPDATE DB CFG FOR testdb USING LOGFILSIZ 10000;

-- 기본 로그 경로
UPDATE DB CFG FOR testdb USING NEWLOGPATH /db2/logs/testdb/;
```

---

## 4. 네트워크 최적화

### 4.1 TCP 튜닝

#### 4.1.1 Linux 커널 파라미터

```bash
# /etc/sysctl.conf

# TCP 창 크기
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 65536 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728

# TCP 창 크기 확장
net.ipv4.tcp_window_scaling = 1

# TCP 선택적 확인
net.ipv4.tcp_sack = 1

# 재사용 TIME_WAIT 소켓
net.ipv4.tcp_tw_reuse = 1

# TIME_WAIT 소켓 빠른 재활용
net.ipv4.tcp_fin_timeout = 30

# SYN 큐
net.ipv4.tcp_max_syn_backlog = 8192

# 포트 범위
net.ipv4.ip_local_port_range = 1024 65535

# 적용
sudo sysctl -p
```

#### 4.1.2 네트워크 대기열

```bash
# 백로그 크기 증가
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.core.netdev_max_backlog=5000
```

### 4.2 지연 최소화

#### 4.2.1 동일한 네트워크

**권장**:
- 테스트 클라이언트와 DB 서버 동일 네트워크
- VLAN 또는 전용 네트워크 사용
- 방화벽 규칙 최소화

#### 4.2.2 DNS 캐싱

```bash
# /etc/hosts에 DB 서버 추가
192.168.1.100 dbhost
```

---

## 5. 운영 체제 튜닝

### 5.1 Linux 튜닝

#### 5.1.1 파일 디스크립터 제한

```bash
# /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536

# 현재 세션에 적용
ulimit -n 65536
```

#### 5.1.2 스왑 사용 최소화

```bash
# vm.swappiness = 1 (거의 스왑하지 않음)
# vm.swappiness = 0 (스왑하지 않음)
sudo sysctl -w vm.swappiness=1

# 영구 적용
echo 'vm.swappiness=1' | sudo tee -a /etc/sysctl.conf
```

#### 5.1.3 I/O 스케줄러

```bash
# SSD용 noop 또는 deadline
echo noop | sudo tee /sys/block/sda/queue/scheduler

# HDD용 cfq
echo cfq | sudo tee /sys/block/sda/queue/scheduler
```

---

### 5.2 Windows 튜닝

#### 5.2.1 전원 설정

```
제어판 → 전원 옵션 → 고성능
```

#### 5.2.2 가상 메모리

```
시스템 속성 → 고급 → 성능 → 가상 메모리
- 초기 크기: RAM의 1.5배
- 최대 크기: RAM의 3배
```

---

## 6. 성능 벤치마킹

### 6.1 기준선 설정

#### 6.1.1 기본 설정 테스트

```bash
python multi_db_load_tester_jdbc.py \
  --db-type postgresql \
  --host localhost \
  --database testdb \
  --user testuser \
  --password testpass \
  --thread-count 100 \
  --test-duration 300 \
  --warmup 30 \
  --output-format csv \
  --output-file baseline.csv
```

#### 6.1.2 결과 분석

**주요 메트릭**:
- 평균 TPS
- P50/P95/P99 레이턴시
- 에러율
- 풀 통계

---

### 6.2 튜닝 비교

#### 6.2.1 A/B 테스트

**테스트 A (기본 설정)**:
```bash
python multi_db_load_tester_jdbc.py \
  --thread-count 100 \
  --test-duration 300 \
  --output-format csv \
  --output-file test_a.csv
```

**테스트 B (튜닝된 설정)**:
```bash
python multi_db_load_tester_jdbc.py \
  --thread-count 200 \
  --batch-size 100 \
  --test-duration 300 \
  --output-format csv \
  --output-file test_b.csv
```

#### 6.2.2 결과 비교

```python
import pandas as pd

# CSV 파일 로드
df_a = pd.read_csv('test_a.csv')
df_b = pd.read_csv('test_b.csv')

# TPS 비교
avg_tps_a = df_a['TPS'].mean()
avg_tps_b = df_b['TPS'].mean()

print(f"테스트 A 평균 TPS: {avg_tps_a:.2f}")
print(f"테스트 B 평균 TPS: {avg_tps_b:.2f}")
print(f"TPS 개선: {((avg_tps_b - avg_tps_a) / avg_tps_a * 100):.2f}%")
```

---

## 7. 성능 문제 진단

### 7.1 병목 식별

#### 7.1.1 CPU 병목

**증상**:
- CPU 사용량 > 80%
- 높은 스레드 수에도 TPS 증가 없음

**해결 방법**:
- 스레드 수 감소
- 데이터베이스 쿼리 최적화
- 병렬 처리 증가

#### 7.1.2 I/O 병목

**증상**:
- 높은 디스크 I/O 대기 시간
- P99 레이턴시 높음

**해결 방법**:
- SSD 사용
- 파티셔닝 활용
- 인덱스 최적화
- 버퍼 풀 증가

#### 7.1.3 네트워크 병목

**증상**:
- 높은 네트워크 지연
- 패킷 손실

**해결 방법**:
- 동일 네트워크 사용
- 네트워크 대역폭 증가
- TCP 튜닝

#### 7.1.4 메모리 병목

**증상**:
- 높은 메모리 사용량
- 스와핑 발생

**해결 방법**:
- 메모리 증가
- 스레드 수 감소
- 배치 크기 감소

---

### 7.2 성능 프로파일링

#### 7.2.1 데이터베이스 프로파일링

**PostgreSQL**:
```sql
-- pg_stat_statements 활성화
CREATE EXTENSION pg_stat_statements;

-- 상위 10개 느린 쿼리
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;
```

**MySQL**:
```sql
-- 느린 쿼리 로그 확인
SHOW VARIABLES LIKE 'slow_query_log';

-- 프로세스 목록
SHOW FULL PROCESSLIST;
```

**Oracle**:
```sql
-- AWR 리포트 생성
SELECT * FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(...));
```

#### 7.2.2 Python 프로파일링

```python
import cProfile
import pstats

# 프로파일링
profiler = cProfile.Profile()
profiler.enable()

# 테스트 실행
# ...

profiler.disable()

# 결과 분석
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)
```

---

## 8. 모범 사례

### 8.1 튜닝 워크플로우

1. **기준선 설정**
   - 기본 설정으로 테스트
   - 결과 저장

2. **단계별 최적화**
   - 연결 풀 튜닝
   - 스레드 수 튜닝
   - 배치 크기 튜닝
   - 데이터베이스 파라미터 튜닝

3. **성능 비교**
   - 각 단계별 결과 비교
   - 개선율 계산

4. **최종 검증**
   - 안정성 테스트
   - 장기 실행 테스트

### 8.2 튜닝 체크리스트

- [ ] 연결 풀 크기 최적화
- [ ] 스레드 수 최적화
- [ ] 배치 크기 최적화
- [ ] 데이터베이스 버퍼 풀 튜닝
- [ ] 인덱스 생성 및 최적화
- [ ] 파티셔닝 활용
- [ ] 네트워크 지연 최소화
- [ ] TCP 파라미터 튜닝
- [ ] OS 파일 디스크립터 제한
- [ ] 워밍업 기간 적절 설정
- [ ] 모니터링 간격 최적화
- [ ] 성능 기준선 설정
- [ ] 결과 비교 및 분석

---

## 9. 추가 리소스

### 9.1 데이터베이스 문서

- **Oracle**: Oracle Database Performance Tuning Guide
- **PostgreSQL**: PostgreSQL Performance Tuning
- **MySQL**: MySQL Performance Tuning
- **SQL Server**: SQL Server Performance Tuning
- **Tibero**: Tibero Performance Guide
- **IBM DB2**: DB2 Performance Tuning

### 9.2 관련 문서

- README.md: 프로젝트 개요
- TRD.md: 기술 요구사항
- API-Documentation.md: API 문서
- Troubleshooting-Guide.md: 문제 해결 가이드

---

**문서 종료**
