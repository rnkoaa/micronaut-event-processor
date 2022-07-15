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
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

@Testcontainers
@MicronautTest(environments = {"kafka", "test"})
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProductEventKafkaListenerTest implements TestPropertyProvider {
    private static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";
    private static final String ORDER_STREAM_TOPIC = "product-stream-test";
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter";

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));

    @Inject
    private KafkaEventPublisher kafkaEventPublisher;

    @Inject
    private EventRecordCollector eventRecordCollector;

    @Inject
    private ProductEventKafkaListener productEventKafkaListener;

//    private static final Collection<EventRecord> eventRecordReceiver = new ConcurrentLinkedDeque<>();
//    private static final Collection<EventRecord> deadLetterEventRecordReceiver = new ConcurrentLinkedDeque<>();

    @Override
    public @NotNull Map<String, String> getProperties() {
        return Map.of(
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "product.stream.topic", ORDER_STREAM_TOPIC,
            "app.event.dead-letter", APP_EVENT_DEAD_LETTER
        );
    }


    @Test
    void canProductConsumerRecord() throws IOException {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);
        kafkaEventPublisher.publish(ORDER_STREAM_TOPIC, productId, eventRecord);

        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> eventRecordCollector.size() > 0);

        EventRecord receivedRecord = eventRecordCollector.next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

   /* @BeforeEach
    void setup() {
    }

    private static final String TOPIC = "product-stream-test";
    //
    @Inject
    KafkaEventPublisher eventRecordPublisher;

    @Inject
    ProductEventKafkaListener productEventKafkaListener;

    private static final Collection<ConsumerRecord<String, byte[]>> received = new ConcurrentLinkedDeque<>();

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    ResourceLoader resourceLoader;


    @Test
    void twoEventsWithSamePartitionKeyShouldBeConsumedConsecutively() throws IOException {
        var productId = UUID.randomUUID();

        var correlationId = UUID.randomUUID();

        var productCreated = new ProductCreatedEvent(productId, "Product 1");
        var productUpdated = new ProductUpdatedEvent(productId, "Product 1");

        var productCreatedEventRecord = new EventRecord(UUID.randomUUID(), "test-instance", productCreated, new EventMetadata(correlationId));
        var productUpdatedEventRecord = new EventRecord(UUID.randomUUID(), "test-instance", productUpdated, new EventMetadata(correlationId));

        eventRecordPublisher.publish(TOPIC, productId, productCreatedEventRecord);
        eventRecordPublisher.publish(TOPIC, productId, productUpdatedEventRecord);

        await().atMost(5, SECONDS).until(() -> !received.isEmpty());
        assertThat(received.size()).isEqualTo(2);

        Iterator<ConsumerRecord<String, byte[]>> iterator = received.iterator();
        ConsumerRecord<String, byte[]> firstConsumerRecord = iterator.next();
        assertThat(firstConsumerRecord).isNotNull();

        ConsumerRecord<String, byte[]> secondConsumerRecord = iterator.next();
        assertThat(secondConsumerRecord).isNotNull();

        assertThat(firstConsumerRecord.partition()).isEqualTo(secondConsumerRecord.partition());

        EventRecord firstConsumedRecord = objectMapper.readValue(firstConsumerRecord.value(), EventRecord.class);
        assertThat(firstConsumedRecord).isNotNull();

        Object firstData = firstConsumedRecord.getTypedData(firstConsumedRecord.eventClass());
        assertThat(firstData.getClass()).isEqualTo(ProductCreatedEvent.class);

        EventRecord secondConsumedRecord = objectMapper.readValue(secondConsumerRecord.value(), EventRecord.class);
        assertThat(secondConsumedRecord).isNotNull();

        Object secondData = secondConsumedRecord.getTypedData(secondConsumedRecord.eventClass());
        assertThat(secondData.getClass()).isEqualTo(ProductUpdatedEvent.class);
    }

    @AfterEach
    void cleanup() {
        received.clear();
    }*/

    /*
     override fun getProperties(): Map<String, String> {
        return mapOf(
            "kafka.bootstrap.servers" to kafka.bootstrapServers,
            "app.event.topic" to orderEventTopic,
            "app.event.dead-letter.topic" to orderDeadLetterEventTopic
        )
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Primary
    class DeadLetterTopicListener {

        @Topic(orderDeadLetterEventTopic)
        fun receive(eventRecord: EventRecord) {
            deadLetterReceived.add(eventRecord)
        }

    }

    @Primary
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Replaces(EventRecordProcessor::class)
    class EventRecordReceiver {

        //        @Topic("order-stream-test")
        @Topic(orderEventTopic)
        fun receive(eventRecord: EventRecord) {
            println("Got Message $eventRecord")
            eventRecordReceived.add(eventRecord)
        }
    }
     */
   /* @KafkaListener(offsetReset = EARLIEST)
    static class EventRecordListener {

        @Topic(TOPIC)
        void processEvent(ConsumerRecord<String, byte[]> record) {
            received.add(record);
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