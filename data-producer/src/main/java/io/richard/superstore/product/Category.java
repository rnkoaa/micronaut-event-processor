package io.richard.superstore.product;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record Category(String name,
                       @JsonProperty("sub_category")
                       String subCategory,
                       List<Category> categories) {

    public Category(String name, String subCategory) {
        this(name, subCategory, List.of());
    }
}
