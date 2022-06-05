package io.richard.superstore.order;

import io.richard.superstore.product.Product;
import java.math.BigDecimal;
import java.time.Instant;

public record Order(
    String id,
    Instant orderDate,
    Instant shipDate,
    ShipmentMode shipmentMode,
    Customer customer,
    ProductSegment segment,
    Address deliveryAddress,
    Product product,
    int quantity,
    BigDecimal discount,
    BigDecimal profit
) {
}
