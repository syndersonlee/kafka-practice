package org.swm.kafkapractice.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "processed_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEvent {

    @Id
    @Column(length = 100)
    private String eventId;     // Kafka 헤더의 event-id (UUID)

    @Column(nullable = false, length = 100)
    private String topic;

    @Column(nullable = false)
    private Instant processedAt;

    public ProcessedEvent(String eventId, String topic) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        this.eventId = eventId;
        this.topic = topic;
        this.processedAt = Instant.now();
    }
}
