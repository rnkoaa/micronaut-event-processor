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

}
