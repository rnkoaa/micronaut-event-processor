package io.richard.event;

import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class EventRecordKafkaListenerTest extends AbstractKafkaTest {
    private static final String ORDER_STREAM_TOPIC = "app-product-stream-test";
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter";

    @Inject
    private KafkaEventPublisher kafkaEventPublisher;

    @Inject
    private EventCollector eventCollector;

    @Inject
    private EventRecordKafkaListener eventRecordKafkaListener;

    @Test
    void testProductEventInjected() {
        assertThat(kafkaContainer.isRunning()).isTrue();
        assertThat(eventRecordKafkaListener).isNotNull();
        assertThat(kafkaEventPublisher).isNotNull();
        assertThat(eventCollector).isNotNull();
    }

    @Override
    protected Map<String, String> additionalProperties() {
        return Map.of(
            "app.event.topic", ORDER_STREAM_TOPIC,
            "app.event.dead-letter.topic", APP_EVENT_DEAD_LETTER
        );
    }

    @Test
    void assertKafkaIsRunning() {
        assertThat(kafkaContainer.isRunning()).isTrue();
    }

    @Test
    void canProductConsumerRecord() {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);
        kafkaEventPublisher.publish(productId, eventRecord);

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> eventCollector.size() > 0);

        ProductCreatedEvent receivedProductCreatedEvent = (ProductCreatedEvent) eventCollector.next();
        assertThat(receivedProductCreatedEvent).isEqualTo(productCreatedEvent);
    }

    @Test
    void canConsumeProductUpdatedEvent() {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productUpdatedEvent = new ProductUpdatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productUpdatedEvent, eventMetadata);
        kafkaEventPublisher.publish(productId, eventRecord);

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> eventCollector.size() > 0);

        ProductUpdatedEvent receivedProductCreatedEvent = (ProductUpdatedEvent) eventCollector.next();
        assertThat(receivedProductCreatedEvent).isEqualTo(productUpdatedEvent);
    }

    @AfterEach
    void afterEach() {
        eventCollector.clear();
    }

}