package io.richard.superstore.product;

public record Product(
    String sku,
    String name,
    Category category
) {
}
