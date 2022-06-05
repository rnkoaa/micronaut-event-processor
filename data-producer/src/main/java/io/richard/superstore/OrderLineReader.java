package io.richard.superstore;

import com.fasterxml.jackson.databind.MappingIterator;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class OrderLineReader {

    public static List<OrderLine> read(String filepath) {
        Objects.requireNonNull(filepath, "filepath cannot be null");
        if (filepath.isEmpty()) {
            return List.of();
        }

        try (FileInputStream fileInputStream = new FileInputStream(filepath)) {
            MappingIterator<OrderLine> objectMappingIterator = ObjectMapperBuilder.buildCsvObjectMapper()
                .readerFor(OrderLine.class)
                .with(OrderLineSchema.build())
                .readValues(fileInputStream);

            return objectMappingIterator.readAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
