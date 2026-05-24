package org.swm.kafkapractice.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final int  MAX_ATTEMPTS = 10;
    private static final long SEND_TIMEOUT_SEC = 5L;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPending() {
        List<OutboxEntry> pending = outboxRepository.findTop100ByPublishedFalseOrderByIdAsc();
        if (pending.isEmpty()) {
            return;
        }

        for (OutboxEntry entry : pending) {
            if (entry.getAttempts() >= MAX_ATTEMPTS) {
                log.error("Outbox row exhausted retries. id={}, topic={}, key={}",
                        entry.getId(), entry.getTopic(), entry.getAggregateId());
                // 운영에서는 별도 dead_outbox 테이블로 이관 + 알람
                continue;
            }

            if (sendOne(entry)) {
                entry.markPublished();
            } else {
                entry.incrementAttempts();
                // 같은 key 순서 유지 위해 한 row 실패 시 멈춤
                break;
            }
        }
    }

    private boolean sendOne(OutboxEntry entry) {
        try {
            kafkaTemplate.send(entry.getTopic(), entry.getAggregateId(), entry.getPayload())
                    .get(SEND_TIMEOUT_SEC, TimeUnit.SECONDS);
            log.info("Outbox published. id={}, topic={}, key={}",
                    entry.getId(), entry.getTopic(), entry.getAggregateId());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Outbox publish interrupted. id={}", entry.getId(), e);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.warn("Outbox publish failed (will retry). id={}, attempt={}",
                    entry.getId(), entry.getAttempts(), e);
            return false;
        }
    }
}
