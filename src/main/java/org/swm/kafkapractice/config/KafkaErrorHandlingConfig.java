package org.swm.kafkapractice.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

@Slf4j
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaOperations<String, Object> template) {
        // 1) DLT 발행 — 원본 partition 유지
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );

        // 2) 1초 → 2초 → 4초 → 8초 exponential backoff, 누적 15초까지만 재시도
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxInterval(30_000L);
        backOff.setMaxElapsedTime(15_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // 3) 재시도 의미 없는 영구 오류는 즉시 DLT
        handler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                DeserializationException.class
        );

        // 4) 재시도 로깅
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry #{} for offset={}, key={}",
                        deliveryAttempt, record.offset(), record.key(), ex)
        );

        return handler;
    }
}
