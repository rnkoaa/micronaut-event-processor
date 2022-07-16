package io.richard.event;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Primary;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import jakarta.inject.Inject;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@Testcontainers
@MicronautTest(environments = "kafka", rebuildContext = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaEventPublisherTest implements TestPropertyProvider {
    private static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";
    private static final String ORDER_STREAM_TOPIC = "order-stream";
    private static final String ORDER_STREAM_DEAD_LETTER_TOPIC = "order-stream-dead-letter";

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));

    @Inject
    private KafkaEventPublisher kafkaEventPublisher;

    private static final Collection<EventRecord> eventRecordReceiver = new ConcurrentLinkedDeque<>();
    private static final Collection<EventRecord> deadLetterEventRecordReceiver = new ConcurrentLinkedDeque<>();

    @Test
    void assertKafkaIsRunning() {
        assertThat(kafkaContainer.isRunning()).isTrue();
    }

    @Test
    void recordsPublishedWillBeSeenOnSameTopic() {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);
        kafkaEventPublisher.publish(productId, eventRecord);

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> eventRecordReceiver.size() > 0);

        EventRecord receivedRecord = eventRecordReceiver.iterator().next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

    @Test
    void deadLetterEventsWillBeReadOnDeadLetterTopic() {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);

        kafkaEventPublisher.publish( productId, eventRecord);

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> deadLetterEventRecordReceiver.size() > 0);

        EventRecord receivedRecord = deadLetterEventRecordReceiver.iterator().next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

    @AfterEach
    void afterEach() {
        eventRecordReceiver.clear();
    }

    @Override
    public @NotNull Map<String, String> getProperties() {
        return Map.of(
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "product.stream.topic", "product-stream-test"
        );
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
//    @Primary
    static class EventRecordListener {

        @Topic(ORDER_STREAM_TOPIC)
        void receive(EventRecord eventRecord) {
            eventRecordReceiver.add(eventRecord);
        }

        @Topic(ORDER_STREAM_DEAD_LETTER_TOPIC)
        void receiveDeadLetter(EventRecord eventRecord) {
            deadLetterEventRecordReceiver.add(eventRecord);
        }
    }
}