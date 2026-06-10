# JobLens — 백엔드 포트폴리오

> 채용 공고 수집·검색·알림·트렌드 분석 백엔드  
> Java 21 · Spring Boot 3.4 · PostgreSQL 16 · Redis 7 · Elasticsearch 8.15 · Docker · GitHub Actions · Prometheus/Grafana

**GitHub**: https://github.com/N0067H/joblens-backend

---

## 1. Problem

개발자가 채용 공고를 탐색할 때 겪는 세 가지 불편:

| 문제 | 현실 | 원하는 것 |
|------|------|---------|
| 분산 플랫폼 | 원티드·잡코리아·링크드인을 매일 직접 확인 | 한 곳에서 통합 검색 |
| 알림 부재 | 원하는 스택 공고가 올라와도 즉시 모름 | 조건 매칭 즉시 알림 |
| 시장 파악 | "요즘 Go가 뜨는지" 감에 의존 | 기술 태그 트렌드 데이터 |

→ **수집 → 태그 추출 → 검색 → 알림** 파이프라인을 구축하기로 결정.

> **Mock 데이터 사용 이유**: 채용 플랫폼 약관상 크롤러 연동 불가. 현재 375건의 시대별 분산 Mock 데이터(Wave A~D)로 수집 구조·이벤트 파이프라인·ES 색인 흐름을 검증. 실 크롤러 연동 시 코드 변경 없이 `JobCollector` 구현체 교체만으로 동작.

---

## 2. Architecture

### 시스템 구성도

```
┌──────────────┐    HTTPS     ┌────────────────────────────────────────────┐
│  Next.js 15  │─────────────▶│  Spring Boot 3.4  (port 8081)              │
│  App Router  │              │                                            │
│  TanStack Q  │              │  ┌────────────┐   ┌──────────────────────┐ │
│  Zustand     │              │  │ Controller │──▶│   Service Layer      │ │
└──────────────┘              │  └────────────┘   └──────────┬───────────┘ │
                              │                              │             │
                              │  ┌───────────────────────────▼──────────┐  │
                              │  │   Repository (JPA + QueryDSL + ES)   │  │
                              │  └──────┬──────────────┬────────────────┘  │
                              └─────────┼──────────────┼───────────────────┘
                                        │              │
                          ┌─────────────▼──┐  ┌────────▼──────────────────┐
                          │ PostgreSQL 16   │  │ Redis 7   │ ES 8.15+nori  │
                          └────────────────┘  └───────────────────────────┘
                                                          ▲
                          ┌────────────────────────────────┘
                          │  Prometheus (scrape /actuator/prometheus)
                          │  Grafana (dashboard + alert rules)
                          └────────────────────────────────────────────────
```

### 이벤트 기반 비동기 파이프라인

Spring `ApplicationEvent` + `@TransactionalEventListener(AFTER_COMMIT)` + `@Async` + `REQUIRES_NEW` 조합.  
도메인 간 직접 의존 없이 기능을 추가할 수 있음.

```
JobCollectService.collectAndSave()
  └─▶ publish(JobCollectedEvent)
        ├─▶ TechTagExtractor      (Claude API → 기술 태그 추출)
        │     └─▶ publish(JobIndexedEvent)
        │               └─▶ EsIndexListener  (태그 포함 상태로 ES 색인)
        ├─▶ AlertMatchService     (알림 조건 매칭 → 이메일)
        └─▶ TrendSnapshotService  (트렌드 스냅샷 갱신)
```

### 검색 이중 엔진 흐름 (Sequence)

```
Client          Spring          Redis           ES              PG
  │── GET /jobs ──▶│
  │               │── CACHE GET ──▶│
  │               │◀── HIT ────────│
  │◀── 응답(3ms) ──│
  │
  │── GET /jobs ──▶│
  │               │── CACHE GET ──▶│
  │               │◀── MISS ───────│
  │               │── ES search ───────────▶│
  │               │◀── results ────────────│  (실패 시)
  │               │                        │── PG fallback ──▶│
  │               │── CACHE SET ──▶│        │                  │
  │◀── 응답 ────── │
```

`search.elasticsearch.enabled=false` 플래그로 PG 단독 운영 가능. ES 장애 시 자동 폴백.

### 패키지 구조

```
com.joblens
├── common/      # ApiResponse, ErrorCode, SecurityConfig, RateLimitFilter
├── domain/
│   ├── auth/    # JWT 발급·검증·갱신
│   ├── job/     # 검색, 수집, 자동완성, ES 문서
│   ├── alert/   # 알림 조건 등록·매칭·이메일
│   ├── trend/   # 기술 태그 트렌드 집계
│   └── bookmark/# 북마크, 지원 단계 관리
├── infra/
│   ├── ai/         # Claude API 기술 태그 추출
│   ├── mail/       # JavaMail 이메일 발송
│   ├── elasticsearch/  # ES 색인, 동기화, 이벤트 리스너
│   └── scheduler/  # 수집·만료·트렌드 스케줄러
└── event/       # JobCollectedEvent, JobIndexedEvent
```

### ERD (핵심 관계)

```
users ──(1:N)── user_alerts          (알림 조건 JSONB)
      ──(1:N)── notifications
      ──(1:N)── application_records  ──(N:1)── job_postings
      ──(M:N)── bookmarked_jobs      ──(N:1)── job_postings

companies ──(1:N)── job_postings ──(M:N)── tech_tags
                                            └──(1:N)── trend_snapshots

tech_tags: id, name, normalized_name, category
```

**핵심 인덱스**

| 인덱스 | 용도 |
|--------|------|
| `GIN(to_tsvector(title‖description))` | PG FTS |
| `(is_active, posted_at DESC)` | 활성 공고 최신순 |
| `GIN(conditions)` | JSONB 알림 조건 |
| `(user_id, is_read, created_at DESC)` | 알림 목록 |

**ES 인덱스 매핑**

```json
{
  "title":        "text / nori_analyzer",
  "description":  "text / nori_analyzer",
  "companyName":  "keyword",
  "techTags":     "keyword[]",
  "location":     "keyword",
  "postedAtEpoch":"long"
}
```

---

## 3. Tech Decisions

### Spring Boot + Java 21 Virtual Thread

공고 수집·Claude API 호출·이메일 발송은 모두 I/O 바운드 작업이다.  
WebFlux는 동일 처리량을 낼 수 있지만 콜백/Mono 체인으로 코드 복잡도가 크게 높아진다.  
Virtual Thread로 기존 명령형 코드를 그대로 유지하면서 수백 개의 동시 I/O를 처리했다.

### Elasticsearch 8.15.4 + nori 형태소 분석기

- **PG `LIKE '%keyword%'`의 한계**: sequential scan, p99 spike, 한국어 분리 불가
- **nori 채택 이유**: 한국어 형태소 분리 (`스타트업` → 검색 가능), 역인덱스로 O(log n) 검색
- **언제 전환했나**: MVP PG FTS에서 96건 p99 spike 237ms 확인 후 → ES 전환 결정

### Redis 다중 용도 활용

| 용도 | 키 패턴 | TTL |
|------|---------|-----|
| 검색 결과 캐시 | `jobs:search:{md5}` | 5분 |
| 트렌드 캐시 | `trends:{period}` | 1시간 |
| 자동완성 | `autocomplete:prefix` Sorted Set | 영구 |
| Rate Limit | `ratelimit:{ip}:{window}` | 1분 |

### PostgreSQL JSONB (알림 조건)

알림 조건은 사용자마다 구성이 다르다. JSONB로 저장해 스키마 변경 없이 새 조건 타입 추가 가능.

```json
{ "techTags": ["Java", "Spring"], "locations": ["서울"], "experienceLevel": "JUNIOR" }
```

### Testcontainers (H2 대신)

H2는 `ON CONFLICT`, JSONB 연산자, `to_tsvector` 등 PostgreSQL 방언을 지원하지 않는다.  
실제 PostgreSQL 16 + Redis 7 컨테이너로 통합 테스트 → 배포 후 DB 관련 예상치 못한 오류 0건.

### Docker + GitHub Actions CI/CD

```yaml
# .github/workflows/ci.yml 흐름
push to main
  → gradle test (Testcontainers 포함)
  → docker build & push (GHCR)
  → EC2 SSH: docker pull & compose up
```

---

## 4. Tradeoffs

### ES vs PG: 소규모에서는 PG가 빠름

ES는 네트워크 왕복 오버헤드(+12ms)가 있어 375건 이하에서는 PG median이 더 빠르다.

| 지표 | PG 375건 | ES 375건 | 설명 |
|------|----------|----------|------|
| p50 | **27ms** | 39ms | PG 우세 |
| p95 | **38ms** | 75ms | PG 우세 |
| 스케일 | O(n) sequential | O(log n) 역인덱스 | 10K+ 이상 ES 역전 |
| 검색 품질 | substring match | 형태소 분석 | ES 우세 |

**결정**: 단기 median 손해를 감수하고 ES 조기 도입. 이유: (1) 한국어 검색 품질, (2) 10K+ 스케일 대비 선제 구조.

### Spring ApplicationEvent vs Kafka

| 항목 | ApplicationEvent (현재) | Kafka |
|------|------------------------|-------|
| 이벤트 유실 | 앱 재시작 시 유실 가능 | 영속적 |
| 복잡도 | 낮음 | 높음 (브로커 운영) |
| 지연 | 동일 프로세스 내 | 네트워크 왕복 |
| 현재 영향 | 6시간 수집 주기, 유실 영향 작음 | — |

**결정**: 현재 규모에서 Kafka 운영 비용이 이득을 초과. 실서비스화 시 트랜잭셔널 아웃박스 패턴으로 전환 예정.

### 캐시 무효화: 패턴 전체 삭제 vs 선택적 삭제

신규 공고 수집 시 `jobs:search:*` 전체 삭제를 선택했다.  
선택적 삭제를 하려면 "어떤 검색 파라미터 조합에 이 공고가 나타날 것인가"를 역산해야 하는데, 이는 예측 불가능하다.  
전체 삭제는 간단하고 일관성이 보장되며, 6시간 수집 주기에서 캐시 재생성 비용이 크지 않다.

### EC2 단일 인스턴스 vs ECS/EKS

포트폴리오 규모에서 ECS/EKS의 운영 복잡도와 비용이 과도하다.  
Docker Compose + EC2 t3.medium으로 전체 스택을 단일 인스턴스에 배포.  
스케일 필요 시 ECS Fargate로 전환 가능한 구조로 작성 (Dockerfile 표준화).

---

## 5. Performance

### 측정 방법론

- n=200 반복, 키워드 25종 순환
- 각 요청 전 `FLUSHDB` (순수 검색 엔진 성능)
- localhost loopback, WSL2 환경

### Phase 1 — Redis 캐시 도입

| 구분 | p50 | p95 | p99 |
|------|-----|-----|-----|
| 캐시 미스 (PG FTS) | 11ms | 24ms | 292ms |
| 캐시 히트 (Redis) | **4ms** | **8ms** | **18ms** |

p95 기준 **24ms → 8ms (67% 감소)**.

### Phase 2 — Elasticsearch + nori 도입 (96건 기준)

| 지표 | PG LIKE | ES + nori | 개선율 |
|------|---------|-----------|--------|
| p50 | 17ms | 20ms | ≈동등 |
| p95 | 32ms | **28ms** | -13% |
| **p99** | **237ms** | **49ms** | **-78%** ✅ |
| max | 329ms | **56ms** | **-83%** ✅ |

### 스케일별 추이 (캐시 미스 기준)

| 데이터 규모 | PG p50 | ES p50 | PG max | ES max |
|------------|--------|--------|--------|--------|
| 96건 (실측) | 17ms | 20ms | 329ms | 56ms |
| 275건 (실측) | 24ms | 35ms | 543ms | 145ms |
| 375건 (실측) | 27ms | 39ms | 65ms* | 211ms |
| 10K건 (예측) | ~200ms | ~85ms | — | — |
| 100K건 (예측) | ~1,500ms | ~100ms | — | — |

*375건 PG max 65ms는 폴백 워밍업 후 측정 (이전 측정은 ES 중지 직후 connection timeout 혼입)

**핵심 인사이트**: PG tail latency(max)는 96→275건 사이에서 329ms→543ms로 급증. ES는 56ms→145ms로 완만. 데이터 증가에 따른 안정성 차이가 스케일 전환 근거.

### Phase 3 — Prometheus + Grafana 모니터링 오버헤드

| 항목 | 값 |
|------|-----|
| `/actuator/prometheus` p99 | 19ms |
| 검색 API 모니터링 오버헤드 | **< 1%** |
| metric time-series 수 | 249개 |
| 커스텀 비즈니스 메트릭 | 7종 |

`Counter.increment()`는 CAS 연산 수준 (< 1μs). 15초 스크랩 간격 기준 앱 처리량 영향 무시.

### Latency 요약 (현재 시스템, 캐시 히트 기준)

| 엔드포인트 | p50 | p95 | p99 |
|-----------|-----|-----|-----|
| 검색 (Redis 히트) | 3ms | 8ms | 19ms |
| 자동완성 | < 3ms | 5ms | 10ms |
| 트렌드 (Redis 히트) | 2ms | 5ms | 12ms |
| 검색 (ES 캐시미스) | 39ms | 75ms | 158ms |

### 커스텀 비즈니스 메트릭 (Prometheus)

| 메트릭 | 설명 |
|--------|------|
| `autocomplete_requests_total` | 자동완성 API 호출 수 |
| `search_cache_hits_total` | 검색 Redis 캐시 히트 |
| `search_cache_misses_total` | 검색 Redis 캐시 미스 |
| `rate_limit_blocked_total{type=general}` | 일반 API 429 차단 |
| `rate_limit_blocked_total{type=auth}` | 인증 API 429 차단 |
| `jobs_collected_total` | 신규 수집 공고 수 |
| `es_indexed_total` | ES 색인 공고 수 |

---

## 6. Failures & Troubleshooting

### 6-1. Cache Strategy Failure — 전체 무효화의 부작용

**현상**: 공고 수집 완료 후 1~2분간 검색 응답이 느려짐.

**원인**: `jobs:search:*` 전체 삭제 후 Redis가 비어 있는 상태에서 모든 검색 요청이 동시에 ES를 조회. 캐시 재생성 중 요청이 집중되는 **Cache Stampede** 패턴 발생.

**해결 (현재)**: 수집은 6시간 주기로 실행. 1~2분 재생성 구간의 레이턴시 증가는 허용 범위로 판단, 단순 전체 삭제 유지.

**프로덕션 대응 방안**: Probabilistic Early Expiration(확률적 선제 갱신) 또는 `SETNX`로 단일 요청만 백엔드 조회 허용하고 나머지는 stale 값 반환 (SWR 패턴).

---

### 6-2. Failure — `@TransactionalEventListener` 트랜잭션 오류

**문제**: `AlertMatchService.onJobCollected()`에서 `TransactionRequiredException` 발생.

```
No existing transaction found for transaction marked with propagation 'mandatory'
```

**원인 분석**:

`@TransactionalEventListener`의 기본 phase는 `AFTER_COMMIT`. 부모 트랜잭션이 이미 커밋·종료된 시점에 실행되므로 `@Transactional` 기본값(REQUIRED)으로는 참여할 트랜잭션이 없다. `@Async`가 붙으면 별도 스레드에서 실행되어 컨텍스트가 완전히 분리된다.

**해결**:

```java
// Before — 부모 트랜잭션 참여 시도 → 실패
@Async
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
@Transactional  // REQUIRED
public void onJobCollected(JobCollectedEvent event) { ... }

// After — 독립 트랜잭션 생성
@Async
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
@Transactional(propagation = Propagation.REQUIRES_NEW)
public void onJobCollected(JobCollectedEvent event) { ... }
```

이후 모든 이벤트 리스너에 일관 적용.

---

### 6-3. Failure — ES 색인에 techTags: [] (Race Condition)

**문제**: ES에서 기술 태그로 검색해도 결과 없음. ES 문서 확인 시 `techTags: []`.

**원인 분석**:

`JobCollectedEvent` 리스너가 `TechTagExtractor`(Claude API, ~2초)와 `EsIndexListener` 두 곳이었다. 비동기 동시 실행으로 태그 추출 전에 ES 색인이 먼저 실행되는 경쟁 조건.

```
JobCollectedEvent
  ├─▶ TechTagExtractor (~2초 소요)
  └─▶ EsIndexListener (즉시 실행 → techTags = [])  ← 문제
```

**해결**: ES 색인 트리거를 `JobCollectedEvent` → `JobIndexedEvent`로 분리.

```
JobCollectedEvent
  └─▶ TechTagExtractor (태그 추출 + 저장 완료)
        └─▶ publish(JobIndexedEvent)
                └─▶ EsIndexListener (태그 포함 상태로 색인)
```

---

### 6-4. Failure — 트렌드 API 항상 빈 배열 반환

**문제**: `GET /api/v1/trends` 응답이 항상 `[]`.

**원인 분석**: 트렌드 스냅샷 생성 시 `periodStart`를 이번 주 월요일(ISO 경계)로 계산.

```java
// Before
LocalDate periodStart = LocalDate.now().with(previousOrSame(MONDAY)); // 2026-06-09
// → WHERE posted_at >= 2026-06-09  → 기존 공고 0건 해당
```

**해결**: ISO 주 경계 → 최근 N일 롤링 윈도우로 변경.

```java
// After
LocalDate periodStart = LocalDate.now().minusWeeks(1);  // 최근 7일
```

---

### 6-5. Failure — nori 형태소 분석기 기술 외래어 과분해

**문제**: `프론트엔드` 검색 시 PG는 4건, ES는 0건.

**원인**: nori가 기술 외래어를 과분해한다. 추가로, 일부 공고는 `Frontend`(영문)로 등록되어 있었다.

```
입력: "프론트엔드"
nori → ['프론트', '엔드']  (색인/검색 모두)
→ 매칭은 이론상 되어야 하지만 영문 등록 공고와 불일치
```

**현재 대응**: `nori_search_analyzer`에 `lowercase` 필터 적용.  
**근본 해결 예정**: `user_dictionary`에 `백엔드/NNG`, `프론트엔드/NNG`, `쿠버네티스/NNG` 등 기술 외래어 등록.

---

## 7. Overengineering / Redesign

### 처음에 과설계한 것 — Claude API 기술 태그 추출

초기 설계에서 Claude API를 사용해 공고 설명에서 기술 태그를 자동 추출하도록 구현했다.  
현재 Mock 데이터는 이미 태그가 명확하게 포함되어 있어 Claude API가 없어도 정규식으로 충분하다.

→ Claude API 연동 코드는 실 크롤러 연동 시 비정형 공고 설명 파싱을 위해 가치가 있지만, 현재 규모에서는 과설계. 테스트 시 Claude API mock 처리가 필요해 복잡도를 높였다.

### 처음에 누락했다가 추가한 것 — ES 색인 이벤트 분리

`JobCollectedEvent`에서 직접 ES 색인을 호출하는 단순 구조로 시작했다.  
Race condition 버그(techTags: [])를 겪은 후 `JobIndexedEvent`로 이벤트를 분리했다.  
처음부터 "태그 추출 완료 후 색인"이라는 순서 의존성을 설계에 반영했어야 했다.

### 현재 구조에서 단순화한 것

- **알림 이메일**: 실시간 푸시 대신 JavaMail 동기 발송. 현재 수집 주기가 6시간이므로 수초 딜레이 허용 가능.
- **Rate Limiting**: Redis 기반 자체 구현. 트래픽 규모상 API Gateway나 Nginx 설정으로도 충분했음.
- **자동완성**: Redis `ZRANGEBYLEX` prefix match. ES completion suggester 대비 단순하지만 기능적으로 충분.

---

## 8. Infrastructure & DevOps

### Docker Compose 스택

```yaml
services:
  app:          # Spring Boot (JAR → Docker image)
  postgres:     # PostgreSQL 16
  redis:        # Redis 7
  elasticsearch:# ES 8.15 + analysis-nori
  prometheus:   # Prometheus (Grafana provisioning)
  grafana:      # Grafana (대시보드 17개 패널)
```

### GitHub Actions CI/CD

```
push to main
  1. gradle test     # Testcontainers (PG+Redis 실제 컨테이너)
  2. docker build    # JAR → Docker image
  3. docker push     # GHCR
  4. EC2 deploy      # SSH → docker pull → compose up
```

### AWS 구성 (배포 환경)

| 리소스 | 스펙 | 용도 |
|--------|------|------|
| EC2 t3.medium | 2 vCPU / 4GB | 앱 + 전체 스택 단일 인스턴스 |
| RDS (선택) | PostgreSQL 16 | 운영 DB 분리 옵션 |
| ECS Fargate (예정) | — | 스케일 아웃 전환 시 |

### Monitoring (Prometheus + Grafana)

- Prometheus scrape: `/actuator/prometheus` 15초 간격
- Grafana 17개 패널: API RPS·레이턴시·에러율, JVM Heap·GC, DB 커넥션풀, Redis 캐시 히트율, 비즈니스 메트릭
- Alert Rules: p95 > 300ms / 5xx > 1% / Heap > 80% / Rate Limit spike

### Logging

| 항목 | 설정 |
|------|------|
| 포맷 | Logback JSON (운영), 컬러 콘솔 (로컬) |
| 레벨 | 도메인 서비스 DEBUG, 외부 라이브러리 WARN |
| 요청 추적 | `X-Request-Id` 헤더 → MDC `requestId` 삽입 → 로그 전파 |
| ES 로그 | `[ES]` prefix: search, cursor, fallback 이벤트 기록 |

---

## 9. Future Improvements

| 항목 | 이유 | 난이도 |
|------|------|--------|
| nori user_dictionary | 기술 외래어 검색 품질 개선 | 낮음 |
| 실 크롤러 연동 | Mock → 실제 채용 공고 데이터 | 중간 |
| 트랜잭셔널 아웃박스 | 앱 재시작 시 이벤트 유실 방지 | 중간 |
| Blue-Green 배포 | Docker Compose → 무중단 배포 | 중간 |
| ES completion suggester | 자동완성 Redis ZRANGEBYLEX → 퍼지 매칭 | 중간 |
| Redis Cluster | 단일 인스턴스 → HA 구성 | 높음 |
| ECS Fargate 전환 | 스케일 아웃·컨테이너 오케스트레이션 | 높음 |
| 검색 Analytics | 검색어 빈도·클릭률 수집 → 랭킹 개선 | 중간 |
