package io.richard.event;

import static io.richard.event.KafkaEventHeaderKeys.KAFKA_CLOUD_EVENT_TRACE_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_CONTENT_TYPE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_CORRELATION_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_EVENT_PRIORITY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_EVENT_TIMESTAMP;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_EVENT_VERSION;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_IS_EVENT_DEAD;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_MAX_RETRY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_NEXT_RETRY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_OBJECT_TYPE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_PARENT_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_PARTITION_KEY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_PUBLISHED_TIMESTAMP;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_RETRY_COUNT;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_SIMPLE_OBJECT_TYPE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_SOURCE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_SOURCE_TOPIC;
import static io.richard.event.annotations.EventMetadata.CONTENT_TYPE_JSON;
import static io.richard.event.annotations.EventRecordHeaders.CONTENT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.CORRELATION_ID;
import static io.richard.event.annotations.EventRecordHeaders.EVENT_PRIORITY;
import static io.richard.event.annotations.EventRecordHeaders.EVENT_TIMESTAMP;
import static io.richard.event.annotations.EventRecordHeaders.EVENT_VERSION;
import static io.richard.event.annotations.EventRecordHeaders.ID;
import static io.richard.event.annotations.EventRecordHeaders.IS_EVENT_DEAD;
import static io.richard.event.annotations.EventRecordHeaders.MAX_RETRY;
import static io.richard.event.annotations.EventRecordHeaders.NEXT_RETRY;
import static io.richard.event.annotations.EventRecordHeaders.OBJECT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.PARENT_ID;
import static io.richard.event.annotations.EventRecordHeaders.PARTITION_KEY;
import static io.richard.event.annotations.EventRecordHeaders.PUBLISHED_TIMESTAMP;
import static io.richard.event.annotations.EventRecordHeaders.RETRY_COUNT;
import static io.richard.event.annotations.EventRecordHeaders.SIMPLE_OBJECT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.SOURCE;
import static io.richard.event.annotations.EventRecordHeaders.SOURCE_TOPIC;

import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.RetryPolicy;
import io.richard.event.annotations.Strings;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public class KafkaRecordHeaders {

    public static Map<String, Object> extractHeaders(ConsumerRecord<String, byte[]> consumerRecord) {
        Map<String, Object> headers = new HashMap<>();
        for (Header next : consumerRecord.headers()) {
            if (next.key().equals(KAFKA_CLOUD_EVENT_TRACE_ID)) {
                headers.put(CORRELATION_ID, UUID.nameUUIDFromBytes(next.value()));
            }
            if (next.key().equals(KAFKA_CORRELATION_ID)) {
                headers.put(CORRELATION_ID, UUID.nameUUIDFromBytes(next.value()));
            }
            if (next.key().equals(KAFKA_PARENT_ID)) {
                headers.put(PARENT_ID, UUID.nameUUIDFromBytes(next.value()));
            }
            if (next.key().equals(KAFKA_IS_EVENT_DEAD)) {
                headers.put(IS_EVENT_DEAD, Boolean.parseBoolean(new String(next.value())));
            }
            if (next.key().equals(KAFKA_SOURCE_TOPIC)) {
                headers.put(SOURCE_TOPIC, new String(next.value()));
            }
            if (next.key().equals(KAFKA_ID)) {
                headers.put(ID, new String(next.value()));
            }
            if (next.key().equals(KAFKA_CONTENT_TYPE)) {
                headers.put(CONTENT_TYPE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_EVENT_TIMESTAMP)) {
                headers.put(EVENT_TIMESTAMP, Instant.parse(new String(next.value())));
            }
            if (next.key().equals(KAFKA_PUBLISHED_TIMESTAMP)) {
                headers.put(PUBLISHED_TIMESTAMP, Instant.parse(new String(next.value())));
            }

            if (next.key().equals(KAFKA_OBJECT_TYPE)) {
                headers.put(OBJECT_TYPE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_EVENT_VERSION)) {
                headers.put(EVENT_VERSION, Integer.valueOf(new String(next.value())));
            }
            if (next.key().equals(KAFKA_EVENT_PRIORITY)) {
                headers.put(EVENT_PRIORITY, Integer.valueOf(new String(next.value())));
            }
            if (next.key().equals(KAFKA_SOURCE)) {
                headers.put(SOURCE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_PARTITION_KEY)) {
                headers.put(PARTITION_KEY, new String(next.value()));
            }
            if (next.key().equals(KAFKA_SIMPLE_OBJECT_TYPE)) {
                headers.put(SIMPLE_OBJECT_TYPE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_MAX_RETRY)) {
                headers.put(MAX_RETRY, Integer.parseInt(new String(next.value())));
            }
            if (next.key().equals(KAFKA_RETRY_COUNT)) {
                headers.put(RETRY_COUNT, Integer.parseInt(new String(next.value())));
            }
            if (next.key().equals(KAFKA_NEXT_RETRY)) {
                headers.put(NEXT_RETRY, Instant.parse(new String(next.value())));
            }
        }

        headers.putIfAbsent(EVENT_VERSION, 1);
        headers.putIfAbsent(RETRY_COUNT, 0);
        headers.putIfAbsent(MAX_RETRY, 3);
        headers.putIfAbsent(CONTENT_TYPE, "application/json");
        headers.putIfAbsent(CORRELATION_ID, UUID.randomUUID());
        headers.putIfAbsent(ID, UUID.randomUUID());
        headers.putIfAbsent(PUBLISHED_TIMESTAMP, Instant.now().toString());
        return headers;
    }

    public static EventMetadata fromKafkaEventHeaders(Map<String, Object> headers) {
        Objects.requireNonNull(headers, "headers cannot be null");
        var retry = new RetryPolicy(
            (Integer) headers.getOrDefault(MAX_RETRY, 3),
            (Integer) headers.getOrDefault(RETRY_COUNT, 0),
            (Instant) headers.get(NEXT_RETRY),
            (Boolean) headers.getOrDefault(IS_EVENT_DEAD, false));

        return new EventMetadata(
            (UUID) headers.getOrDefault(CORRELATION_ID, UUID.randomUUID()),
            (String) headers.getOrDefault(CONTENT_TYPE, CONTENT_TYPE_JSON),
            (String) headers.getOrDefault(PARTITION_KEY, ""),
            (UUID) headers.get(PARENT_ID),
            (Instant) headers.getOrDefault(EVENT_TIMESTAMP, Instant.now()),
            (Instant) headers.getOrDefault(PUBLISHED_TIMESTAMP, Instant.now()),
            (String) headers.getOrDefault(OBJECT_TYPE, ""),
            (String) headers.getOrDefault(SIMPLE_OBJECT_TYPE, ""),
            (String) headers.getOrDefault(SOURCE, ""),
            (String) headers.getOrDefault(SOURCE_TOPIC, ""),
            (Integer) headers.getOrDefault(EVENT_PRIORITY, 1),
            (Integer) headers.getOrDefault(EVENT_VERSION, 1),
            retry
        );
    }

    public static Map<String, Object> buildHeaders(UUID id, EventMetadata metadata) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(KAFKA_CLOUD_EVENT_TRACE_ID, metadata.correlationId());
        headers.put(KAFKA_CORRELATION_ID, metadata.correlationId());
        headers.put(KAFKA_SOURCE_TOPIC,
            (Strings.isNotNullAndEmpty(metadata.sourceTopic())) ? metadata.sourceTopic() : "");
        headers.put(KAFKA_SOURCE,
            (Strings.isNotNullAndEmpty(metadata.source())) ? metadata.source() : "");
        headers.put(KAFKA_ID, (id != null) ? metadata.source() : "");
        headers.put(KAFKA_CONTENT_TYPE, metadata.contentType());
        headers.put(KAFKA_EVENT_TIMESTAMP,
            (metadata.eventTimestamp() != null) ? metadata.eventTimestamp() : Instant.now().toString());
        headers.put(KAFKA_PUBLISHED_TIMESTAMP,
            (metadata.publishedTimestamp() != null) ? metadata.publishedTimestamp() : Instant.now().toString());
        headers.put(KAFKA_PARTITION_KEY,
            (Strings.isNotNullAndEmpty(metadata.partitionKey())) ? metadata.partitionKey() : "");

        if (Strings.isNotNullAndEmpty(metadata.objectType())) {
            headers.put(KAFKA_OBJECT_TYPE, metadata.objectType());
        }

        if (Strings.isNotNullAndEmpty(metadata.simpleObjectType())) {
            headers.put(KAFKA_SIMPLE_OBJECT_TYPE, metadata.simpleObjectType());
        }
        headers.put(KAFKA_EVENT_VERSION, metadata.version());
        headers.put(KAFKA_EVENT_PRIORITY, metadata.priority());
        headers.put(KAFKA_IS_EVENT_DEAD, metadata.retry().dead());
        headers.put(KAFKA_MAX_RETRY, metadata.retry().maxRetry());
        headers.put(KAFKA_RETRY_COUNT, metadata.retry().retryCounter());
        headers.put(KAFKA_NEXT_RETRY, metadata.retry().nextRetry());

        return headers;
    }
}
