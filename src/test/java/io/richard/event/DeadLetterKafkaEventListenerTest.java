package io.richard.event;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.ExceptionSummary;
import io.richard.event.error.DeadLetterException;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@Disabled
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadLetterKafkaEventListenerTest extends AbstractKafkaTest {

    private static final String DEAD_LETTER_TOPIC = "product-stream-dead-letter";
    private static final String TOPIC = "product-stream-test";

    private static final Collection<EventRecord> deadLetterEventRecords = new ConcurrentLinkedDeque<>();

    @Inject
    KafkaEventPublisher kafkaEventPublisher;

    @Inject
    DeadLetterEventHandler deadLetterEventHandler;

    @Inject
    DeadLetterProductCreatedEventProcessor deadLetterProductCreatedEventProcessor;

    @Override
    protected Map<String, String> additionalProperties() {
        return Map.of(
            "app.event.topic", TOPIC,
            "app.event.dead-letter", DEAD_LETTER_TOPIC
        );
    }

    @Test
    void deadLetterEventsCanBeHandledOnItsOwnTopic() {
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(UUID.randomUUID(), "Trek Bike Series 7");
        EventRecord eventRecord = new EventRecord(UUID.randomUUID(), "test-app", productCreatedEvent);

        kafkaEventPublisher.publish(productCreatedEvent.productId(), eventRecord);

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> deadLetterEventRecords.size() > 0);

        EventRecord deadLetterEventRecord = deadLetterEventRecords.iterator().next();

        assertThat(deadLetterEventRecord).isNotNull();
        assertThat(deadLetterEventRecord.exceptionSummary()).isNotNull();
        assertThat(deadLetterEventRecord.exceptionSummary().message())
            .isEqualTo("We dont' want to create product");

        Class<?> eventClass = deadLetterEventRecord.eventClass();
        assertThat(eventClass).isNotNull()
            .isEqualTo(ProductCreatedEvent.class);

        Event<?> event = deadLetterEventRecord.getEvent(eventClass);
        assertThat(event).isNotNull();

        assertThat(event).isNotNull();
        assertThat(event.getData()).isEqualTo(productCreatedEvent);
    }

    @AfterEach
    void afterEach() {
        deadLetterEventRecords.clear();
    }


    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
//    @Replaces(EventRecordKafkaListener.class)
    private static class DeadLetterProductCreatedEventProcessor extends EventRecordKafkaListener {

        KafkaEventPublisher kafkaEventPublisher;

        public DeadLetterProductCreatedEventProcessor(KafkaEventPublisher kafkaEventPublisher, EventProcessorGroup eventProcessorGroup) {
            super(kafkaEventPublisher, eventProcessorGroup);
            this.kafkaEventPublisher = kafkaEventPublisher;
        }

        @Override
        @Topic(TOPIC)
        void consumeEvents(ConsumerRecord<String, EventRecord> consumerRecord, Consumer<String, byte[]> kafkaConsumer) {
            EventRecord eventRecord = consumerRecord.value();
            EventMetadata metadata = eventRecord.metadata();
            metadata.withDead(true).withSourceTopic(consumerRecord.topic());

            var deadLetterException = new DeadLetterException("We dont' want to create product");
            var eventRecordCopy = eventRecord.withMetadata(metadata)
                .withException(new ExceptionSummary(deadLetterException));

            // publish dead letter
            kafkaEventPublisher.publishDeadLetter(UUID.randomUUID(), eventRecordCopy);

            kafkaConsumer.commitSync(
                Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                    new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")));
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class DeadLetterEventHandler {

        @Topic(DEAD_LETTER_TOPIC)
        void onReceive(EventRecord eventRecord) {
            deadLetterEventRecords.add(eventRecord);
        }

    }
    /*

    // https://stackoverflow.com/questions/60454589/kafka-topic-unit-testing
    // https://support.huaweicloud.com/intl/en-us/devg-kafka/Kafka-java-demo.html

    // Function to get the Stream
    public static <T> Stream<T> streamFromIterator(Iterator<T> iterator) {

        // Convert the iterator to Spliterator to be reused
        Spliterator<T>
            spliterator = Spliterators
            .spliteratorUnknownSize(iterator, 0);

        // Get a Sequential Stream from spliterator
        return StreamSupport.stream(spliterator, false);
    }*/
}