package io.richard.superstore.product;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.richard.common.jackson.EventRecordModule;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProductTest {

    final ObjectMapper objectMapper = objectMapper();

    @Test
    void productCanBeSerialized() throws JsonProcessingException {
        var product = new Product("FUR-BO-10001798", "Bush Somerset Collection Bookcase",
            new Category("Furniture", "Bookcases"));

        String expectedJson = """
            {"metadata":{"correlation_id":"f3892217-e93c-46dc-a8f6-bfc89047c852","priority":1,"version":1},
            "ce_source":"ProductTest","ce_type":"io.richard.superstore.product.Product",
            "ce_simple_type":"io.richard.superstore.product.Product","ce_id":"6a6f4ae1-4457-45dc-9be5-6a26bccc015e",
            "ce_timestamp":"2022-06-07T05:24:30.356072Z","data":{"sku":"FUR-BO-10001798","name":
            "Bush Somerset Collection Bookcase","category":{"name":"Furniture","sub_category":"Bookcases"}}}
            """;

        var eventRecord = new EventRecord(UUID.fromString("6a6f4ae1-4457-45dc-9be5-6a26bccc015e"),
            "ProductTest", product, new EventMetadata(
            UUID.fromString("f3892217-e93c-46dc-a8f6-bfc89047c852"), "", null, 1, 1
        ));

        String message = objectMapper.writeValueAsString(eventRecord);
        assertThat(message).isNotNull().isNotEmpty();
        System.out.println(message);
    }

    @Test
    void deserializeEventRecord() throws JsonProcessingException {
        String expectedJson = """
            {"metadata":{"correlation_id":"f3892217-e93c-46dc-a8f6-bfc89047c852","priority":1,"version":1},
            "ce_source":"ProductTest","ce_type":"io.richard.superstore.product.Product",
            "ce_simple_type":"io.richard.superstore.product.Product","ce_id":"6a6f4ae1-4457-45dc-9be5-6a26bccc015e",
            "ce_timestamp":"2022-06-07T05:24:30.356072Z","data":{"sku":"FUR-BO-10001798","name":
            "Bush Somerset Collection Bookcase","category":{"name":"Furniture","sub_category":"Bookcases"}}}
            """;

        EventRecord eventRecord = objectMapper.readValue(expectedJson, EventRecord.class);
        assertThat(eventRecord).isNotNull();

        Product product = eventRecord.getTypedData(Product.class);
        System.out.println(product);

        Event<Product> event = eventRecord.getEvent(Product.class);
        System.out.println(event);
    }

    private static ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        // Ignore null values when writing json.
        objectMapper.setSerializationInclusion(Include.NON_DEFAULT);

        // Write times as a String instead of a Long so its human readable.
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new EventRecordModule());
        return objectMapper;
    }

}