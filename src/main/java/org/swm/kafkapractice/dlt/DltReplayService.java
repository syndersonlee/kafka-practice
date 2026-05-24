package org.swm.kafkapractice.dlt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class DltReplayService {

    private static final String REPLAY_GROUP_ID = "orders-dlt-replay-tool";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ConsumerFactory<String, Object> consumerFactory;

    public int replay(DltReplayController.DltReplayRequest request) {
        validate(request);
        int replayed = 0;

        try (Consumer<String, Object> consumer =
                     consumerFactory.createConsumer(REPLAY_GROUP_ID, "replay")) {

            consumer.subscribe(List.of(request.dltTopic()));
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, Object> record : records) {
                if (replayed >= request.maxMessages()) break;
                if (sendOne(record, request.targetTopic())) replayed++;
            }
            consumer.commitSync();
        } catch (Exception e) {
            log.error("DLT replay session failed. dlt={}", request.dltTopic(), e);
            throw new IllegalStateException("DLT replay failed: " + e.getMessage(), e);
        }
        log.info("DLT replay done. dlt={}, target={}, replayed={}",
                request.dltTopic(), request.targetTopic(), replayed);
        return replayed;
    }

    private boolean sendOne(ConsumerRecord<String, Object> record, String targetTopic) {
        try {
            kafkaTemplate.send(targetTopic, record.key(), record.value())
                    .get(5, TimeUnit.SECONDS);
            log.info("Replayed. dltOffset={}, key={}", record.offset(), record.key());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during replay", e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Replay failed for one message. dltOffset={}", record.offset(), e);
            return false;
        }
    }

    private void validate(DltReplayController.DltReplayRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        if (request.dltTopic() == null || request.dltTopic().isBlank()) {
            throw new IllegalArgumentException("dltTopic is required");
        }
        if (request.targetTopic() == null || request.targetTopic().isBlank()) {
            throw new IllegalArgumentException("targetTopic is required");
        }
        if (request.maxMessages() <= 0 || request.maxMessages() > 1000) {
            throw new IllegalArgumentException("maxMessages must be 1..1000");
        }
    }
}
