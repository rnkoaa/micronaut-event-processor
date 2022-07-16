package io.richard.event;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import jakarta.inject.Inject;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Testcontainers
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleProductEventListenerTest implements TestPropertyProvider {
    private static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";
    private static final String ORDER_STREAM_TOPIC = "app-product-stream-test";
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter";

    @Inject
    private KafkaEventPublisher kafkaEventPublisher;

    @Inject
    private EventRecordCollector eventRecordCollector;

    @Inject
    private ProductEventKafkaListener productEventKafkaListener;

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));

    @Test
    void testProductEventInjected() {
        assertThat(kafkaContainer.isRunning()).isTrue();
        assertThat(productEventKafkaListener).isNotNull();
        assertThat(kafkaEventPublisher).isNotNull();
        assertThat(eventRecordCollector).isNotNull();
    }

    @Override
    public @NotNull Map<String, String> getProperties() {
        return Map.of(
            "micronaut.application.name", "producer-test-application",
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "product.stream.topic", ORDER_STREAM_TOPIC,
            "app.event.dead-letter", APP_EVENT_DEAD_LETTER
        );
    }

    @Test
    void assertKafkaIsRunning() {
        assertThat(kafkaContainer.isRunning()).isTrue();
    }

    @Test
    void canProductConsumerRecord() throws IOException {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);
        kafkaEventPublisher.publish(productId, eventRecord);

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> eventRecordCollector.size() > 0);

        EventRecord receivedRecord = eventRecordCollector.next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

}