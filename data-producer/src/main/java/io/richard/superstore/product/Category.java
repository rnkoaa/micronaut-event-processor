package io.richard.superstore.product;

import java.util.List;

public record Category(String name, String subCategory, List<Category> categories) {

    public Category(String name, String subCategory) {
        this(name, subCategory, List.of());
    }
}
