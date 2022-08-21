package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record RetryPolicy(
    @JsonProperty("max_retry")
    int maxRetry,

    @JsonProperty("retry_counter")
    int retryCounter,

    @JsonProperty("next_retry")
    Instant nextRetry,

    boolean dead
) {
    public RetryPolicy() {
        this(5, 0, Instant.now(), false);
    }

    public RetryPolicy incrementRetryCounter() {
        var nextCounter = retryCounter + 1;
        return new RetryPolicy(maxRetry, nextCounter, Instant.now(), dead);
    }

    public RetryPolicy markDead() {
        return new RetryPolicy(maxRetry, retryCounter, Instant.now(), true);
    }
}