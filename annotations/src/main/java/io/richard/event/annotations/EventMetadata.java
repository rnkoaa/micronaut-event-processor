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

    int priority,
    int version,

    boolean dead
) {

    public EventMetadata() {
        this(UUID.randomUUID(), "application/json", null, Instant.now(), 1, 1, false);
    }

    public EventMetadata(UUID correlationId) {
        this(correlationId, "application/json", null, Instant.now(), 1, 1, false);
    }

    public EventMetadata withDead(boolean dead) {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, priority, version, dead);
    }

    public EventMetadata copy() {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, priority, version, dead);
    }

    public EventMetadata withCorrelationId(UUID correlationId) {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, priority, version, dead);
    }

    public EventMetadata withParentId(UUID parentEventId) {
        return new EventMetadata(correlationId, contentType, parentEventId, eventTimestamp, priority, version, dead);
    }
}
