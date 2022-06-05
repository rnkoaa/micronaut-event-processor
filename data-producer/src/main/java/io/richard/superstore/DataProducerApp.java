package io.richard.superstore;

import io.richard.superstore.product.Category;
import io.richard.superstore.product.Product;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DataProducerApp {

    public static void main(String[] args) {
        List<OrderLine> orderLines = OrderLineReader.read("data/orders.csv");

        Set<Product> products = orderLines.stream()
            .map(it -> new Product(it.productId(), it.productName(), new Category(it.category(), it.subCategory())))
            .collect(Collectors.toSet());

        for (Product product : products) {
            System.out.println(product);
        }

        System.out.println(products.size());
    }
}
