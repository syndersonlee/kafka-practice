# kafka-practice — Spring Boot + Kafka 워크샵 실습 프로젝트

3시간 핸즈온 워크샵 실습용 저장소. Spring Boot 단일 앱(`:8080`) ↔ Docker Compose 로 띄운 KRaft 3-broker Kafka 클러스터 (`:9092 / :9094 / :9096`) 구조.

> 워크샵 슬라이드(PPT) 의 `· 복붙` chip 슬라이드를 따라 코드를 한 파일씩 작성하며 진행합니다.

## 단계별 브랜치

| 브랜치 | 실습 단계 | 코드 변화 |
|---|---|---|
| `main` | 시작 골격 | Spring Initializr 결과 + `application.yml` + `docker-compose.yml` |
| `step1-setup-cli` | 실습 01 — 로컬 셋업 + CLI 발행/소비 | 코드 변경 없음 (CLI 만 사용) |
| `step2-spring-basic` | 실습 02 — Spring Producer/Consumer 기본 | `KafkaTopicConfig`, `OrderEvent`, `OrderProducer`, `OrderConsumer`, `OrderController` |
| `step3-producer-failure` | 실습 03 — Producer 장애 / 동기 발행 / outbox 패턴 | `sendSync()` 메소드, `/orders/sync` 엔드포인트, outbox 4파일 (Entity / Repository / Service / Publisher) |
| `step4-consumer-idempotent` | 실습 04 — 멱등 처리 (메모리 + DB 트랜잭션) | `ProcessedEvent` JPA, `OrderProcessingService` @Transactional, OrderConsumer 슬림화, Producer 에 event-id 헤더 |
| `step5-dlq` | 실습 05 — ErrorHandler + DLT + 운영 도구 | `orders.DLT` 토픽 빈, `KafkaErrorHandlingConfig`, `DltHeaders`, `DltAlertConsumer`, `DltReplayController` + `DltReplayService` |
| `step6-broker-failure` | 실습 06 — broker 장애 + Actuator 모니터링 | `KafkaClusterHealthIndicator`, `KafkaMetricsExporter`, Actuator 의존성 |

각 브랜치는 해당 실습이 끝났을 때 가져야 할 누적 상태. 막혔을 때 `git checkout step3-producer-failure` 한 줄로 정답 확인 가능.

## 빠른 시작

```bash
# 1) Kafka 클러스터 띄우기
docker compose up -d
docker compose ps                          # kafka1/2/3 + kafka-ui 모두 (healthy) 확인 (30~60초 소요)

# 2) Kafka UI 확인
open http://localhost:8089

# 3) 실습 1 시작 — CLI 로 토픽 생성·발행·소비
git checkout step1-setup-cli              # 또는 main 에서 시작

# 4) 실습 2 부터는 Spring Boot 앱 실행
./gradlew bootRun
```

## 환경

| 항목 | 값 |
|---|---|
| Java | 17 |
| Spring Boot | 3.5.x |
| Gradle DSL | Kotlin (`build.gradle.kts`) |
| Kafka | Confluent 7.6.1 (KRaft 모드, RF=3, min.insync.replicas=2) |
| 기본 패키지 | `org.swm.kafkapractice` |

## 핵심 토픽

| 토픽 | 사용 시점 | 설정 |
|---|---|---|
| `orders` | 실습 02~06 | 3 partitions, RF=3, `min.insync.replicas=2` |
| `orders.DLT` | 실습 05 | 3 partitions, RF=3 |

## 워크샵 자료

상세한 설명·도식은 별도 워크샵 PPT(`index.html`) 와 `00-intro.md` ~ `06-broker-failure.md` 참고.

각 실습 슬라이드의 `· 복붙` chip 이 붙은 슬라이드 = 해당 브랜치의 코드와 1:1 대응.
