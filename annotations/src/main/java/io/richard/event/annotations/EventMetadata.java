package io.richard.event.annotations;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record EventMetadata(
    @JsonProperty("correlation_id")
    UUID correlationId,

    @JsonProperty("content_type")
    String contentType,

    @JsonProperty("partition_key")
    String partitionKey,

    @JsonProperty("parent_event_id")
    UUID parentEventId,

    @JsonProperty("event_timestamp")
    Instant eventTimestamp,

    @JsonProperty("published_timestamp")
    Instant publishedTimestamp,

    @JsonProperty("object_type")
    String objectType,

    @JsonProperty("simple_object_type")
    String simpleObjectType,

    @JsonProperty("source")
    String source,

    @JsonProperty("source_topic")
    String sourceTopic,

    int priority,
    int version,
    RetryPolicy retry
) {

    public static final String CONTENT_TYPE_JSON = "application/json";

    public EventMetadata {
        if (Strings.isNullOrEmpty(objectType)) {
            throw new IllegalStateException("object type of class cannot be null or empty");
        }

        if (Strings.isNullOrEmpty(simpleObjectType)) {
            simpleObjectType = objectType.replaceFirst(".*\\.", "");
        }

        if (sourceTopic == null) {
            sourceTopic = "";
        }
        if (correlationId == null) {
            correlationId = UUID.randomUUID();
        }
        if (contentType == null) {
            contentType = "application/json";
        }
    }

    public EventMetadata(Class<?> objectTypeClass) {
        this(UUID.randomUUID(), CONTENT_TYPE_JSON, "", null, Instant.now(),
            Instant.now(),
            objectTypeClass.getCanonicalName(),
            objectTypeClass.getSimpleName(),
            "",
            "", 1, 1, new RetryPolicy());
    }

    public EventMetadata(Class<?> objectTypeClass, UUID correlationId) {
        this(correlationId, CONTENT_TYPE_JSON, "", null, Instant.now(),
            Instant.now(),
            objectTypeClass.getCanonicalName(),
            objectTypeClass.getSimpleName(), "", "", 1, 1,
            new RetryPolicy());
    }

    public EventMetadata(Class<?> objectTypeClass, UUID correlationId, String partitionKey) {
        this(correlationId, CONTENT_TYPE_JSON, partitionKey, null, Instant.now(),
            Instant.now(),
            objectTypeClass.getCanonicalName(), objectTypeClass.getSimpleName(),
            "",
            null, 1, 1, new RetryPolicy());
    }

    public EventMetadata copy() {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId,
            eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
    }

    public EventMetadata withCorrelationId(UUID correlationId) {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId,
            eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
    }

    public EventMetadata withParentId(UUID parentEventId) {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId,
            eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
    }

    public EventMetadata withSourceTopic(String sourceTopic) {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId, eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
    }

    public EventMetadata withRetry(RetryPolicy retry) {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId, eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
    }

    public EventMetadata withSource(String source) {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId, eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
    }

    public EventMetadata withPartitionKey(String partitionKey) {
        return new EventMetadata(correlationId, contentType, partitionKey, parentEventId, eventTimestamp,
            publishedTimestamp,
            objectType, simpleObjectType,
            source,
            sourceTopic,
            priority, version, retry);
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
}
