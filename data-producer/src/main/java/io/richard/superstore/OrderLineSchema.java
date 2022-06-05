package io.richard.superstore;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;

public class OrderLineSchema {

    public static CsvSchema build() {
        return CsvSchema.builder()
            .addColumn("id", ColumnType.NUMBER)
            .addColumn("order_id", ColumnType.STRING)
            .addColumn("order_date", ColumnType.STRING)
            .addColumn("ship_date", ColumnType.STRING)
            .addColumn("ship_mode", ColumnType.STRING)
            .addColumn("customer_id", ColumnType.STRING)
            .addColumn("customer_name", ColumnType.STRING)
            .addColumn("segment", ColumnType.STRING)
            .addColumn("country", ColumnType.STRING)
            .addColumn("city", ColumnType.STRING)
            .addColumn("state", ColumnType.STRING)
            .addColumn("postal_code", ColumnType.STRING)
            .addColumn("region", ColumnType.STRING)
            .addColumn("product_id", ColumnType.STRING)
            .addColumn("category", ColumnType.STRING)
            .addColumn("sub_category", ColumnType.STRING)
            .addColumn("product_name", ColumnType.STRING)
            .addColumn("sales", ColumnType.STRING)
            .addColumn("quantity", ColumnType.STRING)
            .addColumn("discount", ColumnType.STRING)
            .addColumn("profit", ColumnType.STRING)
            .setUseHeader(true)
            .build();
    }
}
