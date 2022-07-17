package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.richard.event.annotations.ExceptionSummary;

import java.time.Instant;

public record ErrorContext(
    String sourceTopic,

    @JsonProperty("original_data")
    Object originalData,

    @JsonProperty("created_at")
    Instant createdAt,

    long offset,

    @JsonProperty("message_key")
    Object messageKey,

    int partition,

    ExceptionSummary summary
) {
    public ErrorContext(String message) {
        this(null, null, Instant.now(), 0, null, 0, new ExceptionSummary(message));
    }
}
