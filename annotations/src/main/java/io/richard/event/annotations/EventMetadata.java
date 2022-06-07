package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public record EventMetadata(

    @JsonProperty("correlation_id")
    UUID correlationId,

    @JsonProperty("content_type")
    String contentType,

    @JsonProperty("parent_event_id")
    UUID parentEventId,
    int priority,
    int version
) {

    public EventMetadata() {
        this(UUID.randomUUID(), "application/json", null, 1, 1);
    }
}
