package org.swm.kafkapractice.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.swm.kafkapractice.config.KafkaTopicConfig;
import org.swm.kafkapractice.event.OrderEvent;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void send(OrderEvent event) {
        ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(
                KafkaTopicConfig.ORDERS_TOPIC, event.getOrderId(), event);
        // event-id 헤더 (UUID) — Consumer 측 멱등 키
        record.headers().add(new RecordHeader(
                "event-id",
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send. orderId={}", event.getOrderId(), ex);
                return;
            }
            log.info("Sent. orderId={}, partition={}, offset={}",
                    event.getOrderId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        });
    }

    /**
     * 동기 발행 — broker 장애 결과를 즉시 받아 사용자에게 503 으로 노출.
     * delivery.timeout.ms (60s) 보다 살짝 긴 70초 timeout.
     */
    public void sendSync(OrderEvent event) {
        try {
            SendResult<String, OrderEvent> result = kafkaTemplate
                    .send(KafkaTopicConfig.ORDERS_TOPIC, event.getOrderId(), event)
                    .get(70, TimeUnit.SECONDS);
            log.info("Sent sync. orderId={}, partition={}, offset={}",
                    event.getOrderId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending", e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Send failed permanently. orderId={}", event.getOrderId(), e);
            throw new IllegalStateException("Kafka publish failed", e);
        }
    }
}
