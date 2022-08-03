package io.richard.event

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.richard.event.annotations.*
import io.richard.event.error.RetryableException
import io.richard.event.processor.DeadLetterEventPublisher
import io.richard.event.processor.EventProcessor
import io.richard.event.processor.EventProcessorConfig
import io.richard.event.processor.EventProcessorGroupImpl
import jakarta.annotation.Nonnull
import jakarta.inject.Singleton
import org.awaitility.Awaitility
import spock.lang.Shared

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit

class KafkaRetryEventPublisherSpec extends AbstractKafkaSpec {

    private static final String EVENT_TOPIC = "app-topic"
    private static final String EVENT_RETRY_TOPIC = "app-retry-topic"
    private static final String EVENT_DEADLETTER_TOPIC = "app-dead-letter-topic"
    private static final String EVENT_ERROR_TOPIC = "app-error-topic"
    private static final Collection<EventRecord> eventRecordReceiver = new ConcurrentLinkedDeque<>()
    private static final Collection<EventRecord> deadLetterEventRecordReceiver = new ConcurrentLinkedDeque<>()
    private static final Collection<EventRecord> retryEventRecordReceiver = new ConcurrentLinkedDeque<>()
    private static final Collection<EventRecord> errorEventRecordReceiver = new ConcurrentLinkedDeque<>()

    @Shared
    private KafkaEventPublisher kafkaEventPublisher

    void setupSpec() {
        kafkaEventPublisher = context.getBean(KafkaEventPublisher)
    }

    void cleanup() {
        eventRecordReceiver.clear()
        deadLetterEventRecordReceiver.clear()
        retryEventRecordReceiver.clear()
        errorEventRecordReceiver.clear()
    }

    @Override
    Map<String, Object> getAdditionalProperties() {
        return [
                "app.event.topic"             : EVENT_TOPIC,
                "app.event.retry.topic"       : EVENT_RETRY_TOPIC,
                "app.event.dead-letter.topic" : EVENT_DEADLETTER_TOPIC,
                "app.event.error.topic"       : EVENT_ERROR_TOPIC,
                "should-dead-letter-unhandled": "true"
        ]
    }

    @Override
    String getSpecName() {
        return "KafkaRetryEventPublisherSpec"
    }


    void "retry exceptions will republish message unto retry topic until it succeeds"() {
        given:
        var correlationId = UUID.randomUUID()
        var productId = UUID.randomUUID()
        String productName = "Diaper - Size 4"
        var eventMetadata = new EventMetadata(correlationId, EVENT_TOPIC)
        var productCreatedEvent = new ProductCreatedEvent(productId, productName)
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata)

        when:
        kafkaEventPublisher.publish(productId, eventRecord)
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .until(() -> retryEventRecordReceiver.size() > 0)

        then:
        EventRecord receivedRecord = retryEventRecordReceiver.last
        receivedRecord != null
    }

    @Singleton
    @KafkaEventProcessor(ProductUpdatedEvent.class)
    @Primary
    @Replaces(ProductUpdatedEventProcessor)
    @Requires(property = "spec.name", value = "KafkaRetryEventPublisherSpec")
    static class MockProductUpdatedEventProcessor implements EventProcessor<ProductUpdatedEvent> {

        @Override
        void process(ProductUpdatedEvent event) {
            println "Do nothing"
        }
    }

    @Singleton
    @Primary
    @Requires(property = "spec.name", value = "KafkaRetryEventPublisherSpec")
    static class MockEventProcessorGroup extends EventProcessorGroupImpl implements EventProcessorGroup {
        final DeadLetterEventPublisher deadLetterEventPublisher
        final ProductCreatedEventProcessor productCreatedEventProcessor

        MockEventProcessorGroup(
                ProductCreatedEventProcessor productCreatedEventProcessor,
                DeadLetterEventPublisher deadLetterEventPublisher) {
            super(deadLetterEventPublisher, new EventProcessorConfig(true))
            this.deadLetterEventPublisher = deadLetterEventPublisher
            this.productCreatedEventProcessor = productCreatedEventProcessor
        }

        @Override
        <T> void processEvent(@Nonnull Event<T> event, @Nonnull EventMetadata eventMetadata) {
            // always publish a retry exception
            throw new RetryableException("retry event")
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Primary
    static class EventRecordListener {

        @Topic(EVENT_TOPIC)
        void receive(EventRecord eventRecord) {
            eventRecordReceiver.add(eventRecord)
        }

        @Topic(EVENT_DEADLETTER_TOPIC)
        void receiveDeadLetter(EventRecord eventRecord) {
            deadLetterEventRecordReceiver.add(eventRecord)
        }

        @Topic(EVENT_RETRY_TOPIC)
        void receiveRetryEventRecord(EventRecord eventRecord) {
            retryEventRecordReceiver.add(eventRecord)
        }

//        @Topic(EVENT_ERROR_TOPIC)
//        void receiveErrorEventRecord(EventRecord eventRecord) {
//            errorEventRecordReceiver.add(eventRecord)
//        }
    }
/*

    @Inject


    @Inject
    ProductCreatedEventProcessor productCreatedEventProcessor;

//    @Test
    void assertKafkaIsRunning() {
        assertThat(kafkaContainer.isRunning()).isTrue();
    }

//    @Test
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

//    @Test
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

//    @AfterEach
    void afterEach() {
        eventRecordReceiver.clear();
    }

    @Override
    public @NotNull Map<String, String> getProperties() {
        return Map.of(
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "app.event.topic", "product-stream-test"
        );
    }

//    @MockBean(ProductCreatedEventProcessor.class)
    ProductCreatedEventProcessor productCreatedEventProcessor(){
//        return Mockito.mock(ProductCreatedEventProcessor.class);
        return null;
    }

//    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
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
 */
}