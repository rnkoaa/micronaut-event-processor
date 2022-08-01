package io.richard.event

import io.richard.event.annotations.EventMetadata
import io.richard.event.annotations.EventRecord
import org.testcontainers.shaded.org.awaitility.Awaitility
import spock.lang.Shared

import java.util.concurrent.TimeUnit

class EventRecordKafkaListenerSpec extends AbstractKafkaSpec {

    private static final String ORDER_STREAM_TOPIC = "app-product-stream-test"
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter"

    @Shared
    KafkaEventPublisher kafkaEventPublisher

    @Shared
    EventCollector eventCollector

    void setupSpec() {
        kafkaEventPublisher = context.getBean(KafkaEventPublisher)
        eventCollector = context.getBean(EventCollector)
    }

    void "records published will be seen on same topic"() {
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
                .until(() -> eventCollector.size() > 0)

        then:
        ProductCreatedEvent receivedProductCreatedEvent = (ProductCreatedEvent) eventCollector.next()
        receivedProductCreatedEvent == productCreatedEvent
    }

    void "product updated event can be consumed successfully"() {
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
                .until(() -> eventCollector.size() > 0)

        then:
        ProductUpdatedEvent receivedProductUpdatedEvent = (ProductUpdatedEvent) eventCollector.next()
        receivedProductUpdatedEvent == productUpdatedEvent
    }

    void cleanup() {
        eventCollector.clear()
    }

    @Override
    Map<String, Object> getAdditionalProperties() {
        return [
                "app.event.topic"             : ORDER_STREAM_TOPIC,
                "app.event.dead-letter.topic" : APP_EVENT_DEAD_LETTER,
                "app.event.retry.topic"       : "$ORDER_STREAM_TOPIC-retry",
                "should-dead-letter-unhandled": "false"
        ]
    }

    @Override
    String getSpecName() {
        return "EventRecordKafkaListenerSpec"
    }

}