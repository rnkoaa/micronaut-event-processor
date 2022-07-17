package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record RetryPolicy(
    @JsonProperty("max_retry")
    int maxRetry,

    @JsonProperty("retry_counter")
    int retryCounter,

    @JsonProperty("next_retry")
    Instant nextRetry
) {
    public RetryPolicy() {
        this(5, 0, Instant.now());
    }
}