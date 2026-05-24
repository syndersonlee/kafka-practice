package org.swm.kafkapractice.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.swm.kafkapractice.event.OrderEvent;
import org.swm.kafkapractice.idempotency.ProcessedEvent;
import org.swm.kafkapractice.idempotency.ProcessedEventRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProcessingService {

    private final ProcessedEventRepository processedEventRepository;

    /**
     * 멱등 처리 핵심: ID 저장과 비즈니스 처리가 같은 트랜잭션.
     * - 두 번째 호출은 DataIntegrityViolationException 으로 막힘 → skip
     * - 비즈니스 처리 도중 예외 발생 시 ID 저장도 함께 롤백 → 재처리 가능
     */
    @Transactional
    public ProcessResult process(String eventId, String topic, OrderEvent event) {
        try {
            processedEventRepository.saveAndFlush(new ProcessedEvent(eventId, topic));
        } catch (DataIntegrityViolationException e) {
            log.info("Skip (already processed). eventId={}, orderId={}",
                    eventId, event.getOrderId());
            return ProcessResult.SKIPPED;
        }

        applyBusinessLogic(event);
        return ProcessResult.PROCESSED;
    }

    private void applyBusinessLogic(OrderEvent event) {
        if (event.getAmount() < 0) {
            throw new IllegalArgumentException("Negative amount: " + event.getAmount());
        }
        log.info("Business logic applied. event={}", event);
        // orderRepository.save(...) 등
    }

    public enum ProcessResult {
        PROCESSED, SKIPPED
    }
}
