package org.swm.kafkapractice.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.swm.kafkapractice.event.OrderEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final OrderProcessingService processingService;

    @KafkaListener(topics = "orders", groupId = "order-processor")
    public void consume(ConsumerRecord<String, OrderEvent> record,
                        Acknowledgment ack,
                        @Header(value = "event-id", required = false) String eventIdHeader) {

        String eventId = (eventIdHeader != null && !eventIdHeader.isBlank())
                ? eventIdHeader
                : fallbackKey(record);

        try {
            OrderProcessingService.ProcessResult result =
                    processingService.process(eventId, record.topic(), record.value());
            log.info("Consumed. partition={}, offset={}, eventId={}, result={}",
                    record.partition(), record.offset(), eventId, result);
            ack.acknowledge();
        } catch (IllegalArgumentException e) {
            // 영구 오류 — 실습 05 의 ErrorHandler 가 즉시 DLT 처리
            log.warn("Permanent failure. eventId={}, reason={}", eventId, e.getMessage());
            throw e;
        } catch (Exception e) {
            // 일시 오류 — ErrorHandler 가 재시도
            log.error("Transient failure. eventId={}", eventId, e);
            throw e;
        }
    }

    private String fallbackKey(ConsumerRecord<?, ?> record) {
        // Producer 가 event-id 헤더를 안 보낸 레거시 케이스용 fallback
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }
}
