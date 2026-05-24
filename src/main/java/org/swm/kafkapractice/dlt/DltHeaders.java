package org.swm.kafkapractice.dlt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

public final class DltHeaders {

    public static final String EXCEPTION_FQCN     = "kafka_dlt-exception-fqcn";
    public static final String EXCEPTION_MESSAGE  = "kafka_dlt-exception-message";
    public static final String ORIGINAL_TOPIC     = "kafka_dlt-original-topic";
    public static final String ORIGINAL_PARTITION = "kafka_dlt-original-partition";
    public static final String ORIGINAL_OFFSET    = "kafka_dlt-original-offset";

    private DltHeaders() {}

    public static String exceptionClass(ConsumerRecord<?, ?> record) {
        return asString(record, EXCEPTION_FQCN);
    }

    public static String exceptionMessage(ConsumerRecord<?, ?> record) {
        return asString(record, EXCEPTION_MESSAGE);
    }

    public static String originalTopic(ConsumerRecord<?, ?> record) {
        return asString(record, ORIGINAL_TOPIC);
    }

    public static long originalOffset(ConsumerRecord<?, ?> record) {
        String value = asString(record, ORIGINAL_OFFSET);
        if (value == null) return -1L;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    private static String asString(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
