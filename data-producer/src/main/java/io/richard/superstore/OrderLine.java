package io.richard.superstore;

import com.fasterxml.jackson.annotation.JsonProperty;


public record OrderLine(
    int id,
    @JsonProperty("order_id")
    String orderId,
    @JsonProperty("order_date")
    String orderDate,
    @JsonProperty("ship_date")
    String shipDate,
    @JsonProperty("ship_mode")
    String shipMode,
    @JsonProperty("customer_id")
    String customerId,
    @JsonProperty("customer_name")
    String customerName,
    String segment,
    String country,
    String city,
    String state,
    @JsonProperty("postal_code")
    String postalCode,
    String region,
    @JsonProperty("product_id")
    String productId,
    String category,
    @JsonProperty("sub_category")
    String subCategory,
    @JsonProperty("product_name")
    String productName,
    String sales,
    String quantity,
    String discount,
    String profit
) {
}
