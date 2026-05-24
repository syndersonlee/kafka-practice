package org.swm.kafkapractice.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.swm.kafkapractice.config.KafkaTopicConfig;
import org.swm.kafkapractice.event.OrderEvent;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void send(OrderEvent event) {
        // key = orderId → 같은 주문은 같은 파티션 → 순서 보장
        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // 비동기 콜백에서 절대 throw 하지 말 것. 로깅 후 호출자에게 별도 신호.
                log.error("Failed to send order event. orderId={}, status={}",
                        event.getOrderId(), event.getStatus(), ex);
                return;
            }
            log.info("Sent. orderId={}, partition={}, offset={}",
                    event.getOrderId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        });
    }
}
