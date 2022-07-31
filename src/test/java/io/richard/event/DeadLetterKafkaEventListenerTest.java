package io.richard.event;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventRecord;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

//@MicronautTest
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
class DeadLetterKafkaEventListenerTest extends AbstractKafkaTest {

    private static final String DEAD_LETTER_TOPIC = "product-stream-dead-letter";
    private static final String TOPIC = "product-stream-test";
    private static final String APP_ERROR_TOPIC = "app-event-error-unhandled";

    private static final Collection<EventRecord> deadLetterEventRecords = new ConcurrentLinkedDeque<>();

    @Inject
    KafkaEventPublisher kafkaEventPublisher;

    @Inject
    DeadLetterEventHandler deadLetterEventHandler;

    @Override
    protected Map<String, String> additionalProperties() {
        return Map.of(
            "app.event.topic", TOPIC,
            "app.event.dead-letter.topic", DEAD_LETTER_TOPIC,
            "app.event.error.topic", APP_ERROR_TOPIC,
            "should-dead-letter-unhandled", "true"
        );
    }

    @Test
    void eventWithoutProcessorWillDLQIfTrue() {
        ProductDeactivated productDeactivated = new ProductDeactivated(UUID.randomUUID());
        EventRecord eventRecord = new EventRecord(UUID.randomUUID(), "test-app", productDeactivated);

        kafkaEventPublisher.publish(productDeactivated.productId(), eventRecord);

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> deadLetterEventRecords.size() > 0);

        EventRecord deadLetterEventRecord = deadLetterEventRecords.iterator().next();

        assertThat(deadLetterEventRecord).isNotNull();
        assertThat(deadLetterEventRecord.exceptionSummary()).isNotNull();
        assertThat(deadLetterEventRecord.exceptionSummary().message())
            .contains("no processor found for event ProductDeactivated, moving on.");

        Class<?> eventClass = deadLetterEventRecord.eventClass();
        assertThat(eventClass).isNotNull()
            .isEqualTo(ProductDeactivated.class);

        Event<?> event = deadLetterEventRecord.getEvent(eventClass);
        assertThat(event).isNotNull();

        assertThat(event).isNotNull();
        assertThat(event.getData()).isEqualTo(productDeactivated);
    }

    @AfterEach
    void afterEach() {
        deadLetterEventRecords.clear();
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