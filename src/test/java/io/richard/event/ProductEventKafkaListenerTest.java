package io.richard.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.core.io.ResourceLoader;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@MicronautTest
class ProductEventKafkaListenerTest {

    @BeforeEach
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
    void canProductConsumerRecord() throws IOException {
        String path = "data/product_created_event.json";
        Optional<InputStream> resourceAsStream = resourceLoader.getResourceAsStream(path);
        assertThat(resourceAsStream).isPresent();

        byte[] bytes = resourceAsStream.get().readAllBytes();

        EventRecord eventRecord = objectMapper.readValue(bytes, EventRecord.class);
        eventRecordPublisher.publish(TOPIC, eventRecord.id(), eventRecord);

        await().atMost(5, SECONDS).until(() -> !received.isEmpty());
        assertThat(received.size()).isEqualTo(1);

        ConsumerRecord<String, byte[]> consumerRecord = received.iterator().next();
        System.out.println("Partition: " + consumerRecord.partition());
        assertThat(consumerRecord).isNotNull();

//        Headers headers = consumerRecord.headers();
//        List<String> headersForKey = List.of("ce-source",
//            "ce-type", "ce-timestamp", "ce-id", "ce-trace-id", "ce-correlation-id");
//        Map<String, String> messageHeaders = streamFromIterator(headers.iterator())
//            .filter(it -> headersForKey.contains(it.key()))
//            .collect(Collectors.toMap(Header::key, it -> new String(it.value(), StandardCharsets.UTF_8)));

        EventRecord deserializedValue = objectMapper.readValue(consumerRecord.value(), EventRecord.class);

        assertThat(deserializedValue).isNotNull();

        Object typedData = deserializedValue.getTypedData(eventRecord.eventClass());
        System.out.println(typedData);
        assertThat(typedData.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

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
//
        await().atMost(5, SECONDS).until(() -> !received.isEmpty());
        assertThat(received.size()).isEqualTo(2);
//
        Iterator<ConsumerRecord<String, byte[]>> iterator = received.iterator();
        ConsumerRecord<String, byte[]> firstConsumerRecord = iterator.next();
        System.out.println("Partition: " + firstConsumerRecord.partition());
        assertThat(firstConsumerRecord).isNotNull();

        ConsumerRecord<String, byte[]> secondConsumerRecord = iterator.next();
        System.out.println("Partition: " + secondConsumerRecord.partition());
        assertThat(secondConsumerRecord).isNotNull();

        assertThat(firstConsumerRecord.partition()).isEqualTo(secondConsumerRecord.partition());
//
        EventRecord firstConsumedRecord = objectMapper.readValue(firstConsumerRecord.value(), EventRecord.class);
        assertThat(firstConsumedRecord).isNotNull();
//
        Object firstData = firstConsumedRecord.getTypedData(firstConsumedRecord.eventClass());
        System.out.println(firstData);
        assertThat(firstData.getClass()).isEqualTo(ProductCreatedEvent.class);
//
        EventRecord secondConsumedRecord = objectMapper.readValue(secondConsumerRecord.value(), EventRecord.class);
        assertThat(secondConsumedRecord).isNotNull();
//
        Object secondData = secondConsumedRecord.getTypedData(secondConsumedRecord.eventClass());
        System.out.println(secondData);
        assertThat(secondData.getClass()).isEqualTo(ProductUpdatedEvent.class);
    }

    @AfterEach
    void cleanup() {
        received.clear();
    }

    @KafkaListener(offsetReset = EARLIEST)
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
    }
}