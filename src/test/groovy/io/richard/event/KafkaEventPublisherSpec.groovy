package io.richard.event

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.richard.event.annotations.*
import io.richard.event.error.DeadLetterException
import io.richard.event.processor.DeadLetterEventPublisher
import io.richard.event.processor.EventProcessor
import io.richard.event.processor.EventProcessorConfig
import io.richard.event.processor.EventProcessorGroupImpl
import jakarta.annotation.Nonnull
import jakarta.inject.Singleton
import org.testcontainers.shaded.org.awaitility.Awaitility
import spock.lang.Shared

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit

class KafkaEventPublisherSpec extends AbstractKafkaSpec {

    private static final String ORDER_STREAM_TOPIC = "product-stream-test"
    private static final String ORDER_STREAM_DEAD_LETTER_TOPIC = "order-stream-dead-letter"

    static final Collection<EventRecord> eventRecordReceiver = new ConcurrentLinkedDeque<>()
    static final Collection<EventRecord> deadLetterEventRecordReceiver = new ConcurrentLinkedDeque<>()

    @Shared
    KafkaEventPublisher kafkaEventPublisher

    void setupSpec() {
        kafkaEventPublisher = context.getBean(KafkaEventPublisher)
    }

    void "events can be produced and consumed successfully"() {
        given:
        var correlationId = UUID.randomUUID()
        var productId = UUID.randomUUID()
        String productName = "Diaper - Size 4"
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC)
        var productCreatedEvent = new ProductCreatedEvent(productId, productName)
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata)

        when:
        kafkaEventPublisher.publish(productId, eventRecord)

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> eventRecordReceiver.size() > 0)

        then:
        EventRecord receivedRecord = eventRecordReceiver.last

        Event event = receivedRecord.getEvent(receivedRecord.eventClass())
        Object data = event.getData()

        data.class == ProductCreatedEvent
    }

    void "dead letter events will be read on dead letter topic"() {
        given:
        var correlationId = UUID.randomUUID()
        var productId = UUID.randomUUID()
        String productName = "Diaper - Size 4"
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC)
        var productUpdatedEvent = new ProductUpdatedEvent(productId, productName)
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productUpdatedEvent, eventMetadata)

        when:
        kafkaEventPublisher.publish(productId, eventRecord)
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> deadLetterEventRecordReceiver.size() > 0)

        then:
        EventRecord receivedRecord = deadLetterEventRecordReceiver.last

        Event event = receivedRecord.getEvent(receivedRecord.eventClass())
        Object data = event.getData()
        data != null

        and:
        data == productUpdatedEvent
    }

    void cleanup() {
        eventRecordReceiver.clear()
    }

    @Override
    Map<String, Object> getAdditionalProperties() {
        return [
                "app.event.topic"             : ORDER_STREAM_TOPIC,
                "app.event.dead-letter.topic" : ORDER_STREAM_DEAD_LETTER_TOPIC,
                "app.event.retry.topic"       : "$ORDER_STREAM_TOPIC-retry",
                "should-dead-letter-unhandled": "false",
        ]
    }

    @Override
    String getSpecName() {
        return "KafkaEventPublisherSpec"
    }

    @Primary
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Requires(property = "spec.name", value = "KafkaEventPublisherSpec")
    static class EventRecordListener {
        final DeadLetterEventPublisher deadLetterEventPublisher

        EventRecordListener(DeadLetterEventPublisher deadLetterEventPublisher) {
            this.deadLetterEventPublisher = deadLetterEventPublisher
        }

        @Topic(ORDER_STREAM_TOPIC)
        void receive(EventRecord eventRecord) {
            if (eventRecord.eventClass() == ProductUpdatedEvent) {
                deadLetterEventPublisher.handle(eventRecord)
                return
            }
            eventRecordReceiver.add(eventRecord)
        }

        @Topic(ORDER_STREAM_DEAD_LETTER_TOPIC)
        void receiveDeadLetter(EventRecord eventRecord) {
            deadLetterEventRecordReceiver.add(eventRecord)
        }
    }

    @Singleton
    @KafkaEventProcessor(ProductUpdatedEvent.class)
    @Primary
    @Replaces(ProductUpdatedEventProcessor)
    @Requires(property = "spec.name", value = "KafkaEventPublisherSpec")
    static class MockProductUpdatedEventProcessor implements EventProcessor<ProductUpdatedEvent> {

        @Override
        void process(ProductUpdatedEvent event) {
            throw new DeadLetterException("rejecting event in process")
        }
    }

    @Singleton
    @Primary
    @Requires(property = "spec.name", value = "KafkaEventPublisherSpec")
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
            if (event.eventType == ProductCreatedEvent) {
                productCreatedEventProcessor.process((ProductCreatedEvent) event.data)
                return
            }

            super.processEvent(event, eventMetadata)

        }
    }

}