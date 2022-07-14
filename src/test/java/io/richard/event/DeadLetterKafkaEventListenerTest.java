package io.richard.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.core.io.ResourceLoader;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import io.richard.event.processor.EventProcessor;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static io.micronaut.configuration.kafka.annotation.OffsetReset.LATEST;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@MicronautTest
class DeadLetterKafkaEventListenerTest implements TestPropertyProvider {
    @Override
    public Map<String, String> getProperties() {
        return Map.of(
//            "kafka.bootstrap.servers", kafka.getBootstrapServers()
        );
    }

    /*static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"));

    // https://stackoverflow.com/questions/60454589/kafka-topic-unit-testing
    // https://support.huaweicloud.com/intl/en-us/devg-kafka/Kafka-java-demo.html
    @BeforeEach
    void setup() {

    }

    private static final String DEAD_LETTER_TOPIC = "product-stream-dead-letter";
    private static final String TOPIC = "product-stream-test";
    //
    @Inject
    KafkaEventPublisher eventRecordPublisher;

    @Inject
    ProductEventKafkaListener productEventKafkaListener;

    private static final Collection<ConsumerRecord<String, byte[]>> received = new ConcurrentLinkedDeque<>();
    private static final Collection<ConsumerRecord<String, byte[]>> deadLetterReceived = new ConcurrentLinkedDeque<>();

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    ResourceLoader resourceLoader;

    @Inject
    EventProcessorGroup eventProcessorGroup;

    @BeforeAll
    static void beforeAll(){
        kafka.start();
    }

    @Test
    void testEventWithoutProcessor() throws IOException {
        String path = "data/product_deactivated_event.json";
        Optional<InputStream> resourceAsStream = resourceLoader.getResourceAsStream(path);
        assertThat(resourceAsStream).isPresent();

        byte[] bytes = resourceAsStream.get().readAllBytes();

        EventRecord eventRecord = objectMapper.readValue(bytes, EventRecord.class);
        eventRecord = eventRecord.withMetadata(new EventMetadata());
        assertThat(eventRecord).isNotNull();
        assertThat(eventRecord.data()).isNotNull();
        eventRecordPublisher.publish(TOPIC, eventRecord.id(), eventRecord);

        await().atMost(15, SECONDS).until(() -> !received.isEmpty());
        await().atMost(15, SECONDS).until(() -> !deadLetterReceived.isEmpty());
        assertThat(received.size()).isEqualTo(1);
        assertThat(received.size()).isEqualTo(1);

        Class<?> eventClass = eventRecord.eventClass();
        EventProcessor<?> eventProcessor = (EventProcessor<?>) eventProcessorGroup.get(eventClass);
        assertThat(eventProcessor).isNull();
    }

    @AfterEach
    void cleanup() {
        deadLetterReceived.clear();
        received.clear();
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of(
            "kafka.bootstrap.servers", kafka.getBootstrapServers()
        );
    }

    @AfterAll
    static void afterAll(){
        kafka.stop();
    }

    @KafkaListener(offsetReset = LATEST)
    static class EventRecordListener {

        @Inject
        ProductEventKafkaListener productEventKafkaListener;

        @Topic(DEAD_LETTER_TOPIC)
        void processDeadEvents(ConsumerRecord<String, byte[]> record) {
            deadLetterReceived.add(record);
        }

        @Topic(TOPIC)
        void processEvent(ConsumerRecord<String, byte[]> record, Consumer<String, byte[]> consumer) throws IOException, ClassNotFoundException {
            received.add(record);
            productEventKafkaListener.consumeEvents(record, consumer);
        }

    }

    // Function to get the Stream
    public static <T> Stream<T> streamFromIterator(Iterator<T> iterator) {

        // Convert the iterator to Spliterator
        Spliterator<T>
            spliterator = Spliterators
            .spliteratorUnknownSize(iterator, 0);

        // Get a Sequential Stream from spliterator
        return StreamSupport.stream(spliterator, false);
    }*/
}