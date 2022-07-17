package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

public record EventMetadata(

    @JsonProperty("correlation_id")
    UUID correlationId,

    @JsonProperty("content_type")
    String contentType,

    @JsonProperty("parent_event_id")
    UUID parentEventId,

    @JsonProperty("event_timestamp")
    Instant eventTimestamp,

    @JsonProperty("source_topic")
    String sourceTopic,

    int priority,
    int version,
    RetryPolicy retry,
    boolean dead
) {

    public EventMetadata {
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

    public EventMetadata() {
        this(UUID.randomUUID(), "application/json", null, Instant.now(), "", 1, 1, null, false);
    }

    public EventMetadata(UUID correlationId) {
        this(correlationId, "application/json", null, Instant.now(), "", 1, 1, null, false);
    }

    public EventMetadata(UUID correlationId, String sourceTopic) {
        this(correlationId, "application/json", null, Instant.now(), sourceTopic, 1, 1, null, false);
    }

    public EventMetadata withDead(boolean dead) {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, sourceTopic, priority, version, retry, dead);
    }

    public EventMetadata copy() {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, sourceTopic,
            priority, version, retry, dead);
    }

    public EventMetadata withCorrelationId(UUID correlationId) {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, sourceTopic, priority, version, retry, dead);
    }

    public EventMetadata withParentId(UUID parentEventId) {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, sourceTopic, priority, version, retry,
            dead);
    }

    public EventMetadata withSourceTopic(String sourceTopic) {
        return new EventMetadata(correlationId,
            contentType,
            parentEventId, eventTimestamp, sourceTopic, priority, version, retry,
            dead);
    }

    public EventMetadata withRetry(RetryPolicy retry) {
        return new EventMetadata(correlationId,
            contentType,
            parentEventId,
            eventTimestamp, sourceTopic, priority, version, retry,
            dead);
    }
}
