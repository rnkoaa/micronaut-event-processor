package io.richard.event;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.support.TestPropertyProvider;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Disabled
class ProductEventKafkaListenerTest implements TestPropertyProvider {
    private static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";
    private static final String ORDER_STREAM_TOPIC = "app-product-stream-test";
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter";

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));

    private static ApplicationContext applicationContext;
    private EventRecordKafkaListener eventRecordKafkaListener;
    private KafkaEventPublisher kafkaEventPublisher;
    private EventRecordCollector eventRecordCollector;

    @BeforeAll
    static void beforeAll() {
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer.class, PropertySource.of("test", Map.of(
            "micronaut.application.name", "producer-test-application",
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "app.event.topic", ORDER_STREAM_TOPIC,
            "app.event.dead-letter", APP_EVENT_DEAD_LETTER
        )));
        applicationContext = embeddedServer.getApplicationContext();
    }

    @BeforeEach
    void beforeEach() {
        eventRecordKafkaListener = applicationContext.getBean(EventRecordKafkaListener.class);
        kafkaEventPublisher = applicationContext.getBean(KafkaEventPublisher.class);
        eventRecordCollector = applicationContext.getBean(EventRecordCollector.class);
    }

    @Disabled
    @Test
    void testProductEventInjected() {
        assertThat(kafkaContainer.isRunning()).isTrue();
        assertThat(eventRecordKafkaListener).isNotNull();
        assertThat(kafkaEventPublisher).isNotNull();
        assertThat(eventRecordCollector).isNotNull();
    }

    @Override
    public @NotNull Map<String, String> getProperties() {
        return Map.of(
            "micronaut.application.name", "producer-test-application",
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "app.event.topic", ORDER_STREAM_TOPIC,
            "app.event.dead-letter", APP_EVENT_DEAD_LETTER
        );
    }

    @Disabled
    @Test
    void assertKafkaIsRunning() {
        assertThat(kafkaContainer.isRunning()).isTrue();
    }

    @Disabled
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
            .until(() -> eventRecordCollector.size() > 0);

        EventRecord receivedRecord = eventRecordCollector.next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }
}