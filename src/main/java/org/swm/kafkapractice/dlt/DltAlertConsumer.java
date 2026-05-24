package org.swm.kafkapractice.dlt;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DltAlertConsumer {

    private final MeterRegistry meterRegistry;

    @KafkaListener(topics = "orders.DLT", groupId = "orders-dlt-alert")
    public void onDeadLetter(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        try {
            String exception = DltHeaders.exceptionClass(record);
            String origTopic = DltHeaders.originalTopic(record);
            long   origOffset = DltHeaders.originalOffset(record);

            log.error("[DLT] received. key={}, origTopic={}, origOffset={}, exception={}, message={}",
                    record.key(), origTopic, origOffset, exception,
                    DltHeaders.exceptionMessage(record));

            meterRegistry.counter("kafka.dlt.received",
                            "topic", origTopic != null ? origTopic : "unknown",
                            "exception", exception != null ? exception : "unknown")
                    .increment();

            // 사내 알람 발송 자리 (Slack webhook, PagerDuty REST API 등)
            // notifier.send(...);

            ack.acknowledge();
        } catch (Exception e) {
            // 알람 처리 자체가 실패해도 DLT 가 또 차단되면 안 되므로 ack
            log.error("DLT alert handling failed. key={}", record.key(), e);
            ack.acknowledge();
        }
    }
}
