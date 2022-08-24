package io.richard.event;

import io.richard.event.annotations.ErrorContext;
import io.richard.event.annotations.EventMetadata;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public record DeadLetterEventRecord(
    UUID id,
    Instant timestamp,
    EventMetadata metadata,
    ErrorContext data,
    Map<String, Object> headers
) {

    public DeadLetterEventRecord(ErrorContext errorContext, EventMetadata eventMetadata) {
        this(UUID.randomUUID(), Instant.now(), eventMetadata, errorContext, new HashMap<>());
    }
}
