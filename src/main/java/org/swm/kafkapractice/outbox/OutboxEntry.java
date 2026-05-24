package org.swm.kafkapractice.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "outbox",
       indexes = @Index(name = "idx_outbox_pending", columnList = "published,id"))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String topic;

    @Column(nullable = false, length = 100)
    private String aggregateId;    // = Kafka key

    @Lob
    @Column(nullable = false)
    private String payload;        // 직렬화된 이벤트 JSON

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private boolean published;

    @Column(nullable = false)
    private int attempts;

    public OutboxEntry(String topic, String aggregateId, String payload) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (aggregateId == null || aggregateId.isBlank()) {
            throw new IllegalArgumentException("aggregateId must not be blank");
        }
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("payload must not be blank");
        }
        this.topic = topic;
        this.aggregateId = aggregateId;
        this.payload = payload;
        this.createdAt = Instant.now();
        this.published = false;
        this.attempts = 0;
    }

    public void markPublished() {
        this.published = true;
    }

    public void incrementAttempts() {
        this.attempts++;
    }
}
