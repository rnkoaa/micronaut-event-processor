package io.richard.event.annotations;

import java.time.Instant;

public record RetryPolicy(
    int maxRetry,
    int retryCounter,
    Instant nextRetry
) {}