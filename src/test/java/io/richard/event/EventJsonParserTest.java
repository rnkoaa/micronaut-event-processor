package io.richard.event;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.io.ResourceLoader;
import io.richard.event.annotations.ErrorContext;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.ExceptionSummary;
import io.richard.event.error.DeadLetterEventRecord;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

//@MicronautTest
@Disabled
class EventJsonParserTest {

    @Inject
    ObjectMapper objectMapper;

    @Inject
    ResourceLoader resourceLoader;

    @Test
    void parseProductCreatedEvent() throws IOException {
        String path = "data/product_created_event.json";
        Optional<InputStream> resourceAsStream = resourceLoader.getResourceAsStream(path);
        assertThat(resourceAsStream).isPresent();

        byte[] bytes = resourceAsStream.get().readAllBytes();

        EventRecord eventRecord = objectMapper.readValue(bytes, EventRecord.class);
        Event<?> event = eventRecord.getEvent(eventRecord.eventClass());
        assertThat(event.getEventType()).isEqualTo(ProductCreatedEvent.class);
//
        ProductCreatedEvent data = (ProductCreatedEvent) event.getData();
        assertThat(data).isEqualTo(new ProductCreatedEvent(
            UUID.fromString("9bb3d5a7-30a3-4875-a9d0-13be882bb35d"), "product 1"));
    }

    @Test
    void deadRecordSerDeTest() throws JsonProcessingException {
        var deadRecord = new DeadLetterEventRecord(new ErrorContext("Bad Message"), Map.of());
        String deadRecordStr = objectMapper.writeValueAsString(deadRecord);
        System.out.println(deadRecordStr);

        DeadLetterEventRecord deadLetterEventRecord = objectMapper.readValue(deadRecordStr, DeadLetterEventRecord.class);
        assertThat(deadLetterEventRecord).isNotNull();

        System.out.println(deadLetterEventRecord);
    }

    @Test
    void eventRecordWithExceptionCanBeParsed() throws JsonProcessingException {
        var productCreatedEvent = new ProductCreatedEvent(UUID.randomUUID(), "Trek Bike Series 7");
        var deadEvent = new EventMetadata().withDead(true);
        EventRecord eventRecord = new EventRecord(UUID.randomUUID(),
            "test", productCreatedEvent, deadEvent)
            .withException(new ExceptionSummary("Bad serialization"));

        String messageStr = objectMapper.writeValueAsString(eventRecord);
        assertThat(messageStr).isNotEmpty();
        System.out.println(messageStr);

        // message can be deserialized
        EventRecord overAndBack = objectMapper.readValue(messageStr, EventRecord.class);
        assertThat(overAndBack).isNotNull();
        assertThat(overAndBack.exceptionSummary()).isEqualTo(eventRecord.exceptionSummary());
        assertThat(overAndBack.data()).isEqualTo(eventRecord.data());
        assertThat(overAndBack.metadata()).isEqualTo(eventRecord.metadata());
    }

}