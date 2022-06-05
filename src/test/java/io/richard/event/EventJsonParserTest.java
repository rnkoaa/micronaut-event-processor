package io.richard.event;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.micronaut.core.io.ResourceLoader;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@MicronautTest
class EventJsonParserTest {

    @Inject
    EventJsonParser eventJsonParser;

    @Inject
    ResourceLoader resourceLoader;

    @Test
    void parseProductCreatedEvent() throws IOException, ClassNotFoundException {
        String path = "data/product_created_event.json";
        Optional<InputStream> resourceAsStream = resourceLoader.getResourceAsStream(path);
        assertThat(resourceAsStream).isPresent();

        byte[] bytes = resourceAsStream.get().readAllBytes();

        Event<ProductCreatedEvent> event = eventJsonParser.parse(bytes);
        assertThat(event.getEventType()).isEqualTo(ProductCreatedEvent.class);

        ProductCreatedEvent data = event.getData();
        assertThat(data).isEqualTo(new ProductCreatedEvent(
            UUID.fromString("9bb3d5a7-30a3-4875-a9d0-13be882bb35d"), "product 1"));
    }

}