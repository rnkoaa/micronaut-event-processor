package io.richard.event.error;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.richard.event.annotations.EventMetadata;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record DeadLetterEventRecord(
    UUID id,
    Instant timestamp,
    EventMetadata metadata,
    ErrorContext data,
    Map<String, Object> headers
) {

    public DeadLetterEventRecord(Map<String, Object> headers) {
        this(UUID.randomUUID(), Instant.now(), new EventMetadata(), null, headers);
    }

    public DeadLetterEventRecord(ErrorContext errorContext, Map<String, Object> headers) {
        this(UUID.randomUUID(), Instant.now(), new EventMetadata(), errorContext, headers);
    }
}
