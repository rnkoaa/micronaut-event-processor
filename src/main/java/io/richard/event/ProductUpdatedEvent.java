package io.richard.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public record ProductUpdatedEvent(
    @JsonProperty("product_id")
    UUID productId,

    @JsonProperty("product_name")
    String productName
) {}
