package org.swm.kafkapractice.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.swm.kafkapractice.event.OrderEvent;

@Slf4j
@Component
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "order-processor")
    public void consume(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent event = record.value();
        try {
            log.info("Received. partition={}, offset={}, key={}, event={}",
                    record.partition(), record.offset(), record.key(), event);

            process(event);

            ack.acknowledge();   // 처리 성공 후 수동 커밋
        } catch (Exception e) {
            // ack 안 하면 다음 poll 에서 같은 메시지 재수신 → 무한 루프 위험
            // 실습 05 에서 ErrorHandler 로 정식 처리
            log.error("Failed to process. orderId={}", event.getOrderId(), e);
            throw e;   // 일단 던져서 Spring Kafka 기본 ErrorHandler 가 잡도록
        }
    }

    private void process(OrderEvent event) {
        // 실제로는 DB 저장, 외부 API 호출 등
        log.info("Processed: {}", event);
    }
}
