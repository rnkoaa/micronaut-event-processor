package io.richard.event.annotations;

import java.time.Instant;
import java.util.UUID;

public record TestProductCreated(
    UUID id,
    Instant createdAt,
    Instant updatedAt
) {
}
