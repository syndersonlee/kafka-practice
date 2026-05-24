package org.swm.kafkapractice.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.swm.kafkapractice.event.OrderEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderOutboxService {

    private static final String ORDERS_TOPIC = "orders";

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * 비즈니스 작업과 outbox 적재가 한 트랜잭션에 묶인다.
     * Kafka 가 죽어 있어도 여기서는 신경 안 씀. 실제 발행은 OutboxPublisher 가.
     */
    @Transactional
    public void placeOrder(OrderEvent event) {
        // 1) 비즈니스 처리 자리 — 같은 트랜잭션 (예: orderRepository.save(toOrder(event)))

        // 2) outbox 적재
        try {
            String payload = objectMapper.writeValueAsString(event);
            outboxRepository.save(new OutboxEntry(ORDERS_TOPIC, event.getOrderId(), payload));
            log.info("Outbox queued. orderId={}", event.getOrderId());
        } catch (JsonProcessingException e) {
            // 직렬화 실패는 데이터 자체의 문제 → 트랜잭션 롤백
            throw new IllegalStateException("Failed to serialize OrderEvent", e);
        }
    }
}
