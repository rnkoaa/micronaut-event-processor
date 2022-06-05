package io.richard.superstore.order;

public record Address(
    String city,
    String state,
    String postalCode,
    String region,
    String country
) {
}
