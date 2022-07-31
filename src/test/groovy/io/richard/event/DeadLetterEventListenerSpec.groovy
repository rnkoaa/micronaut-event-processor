package io.richard.event

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import io.richard.event.annotations.Event
import io.richard.event.annotations.EventRecord
import org.testcontainers.shaded.org.awaitility.Awaitility
import spock.lang.Shared

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit

class DeadLetterEventListenerSpec extends AbstractKafkaSpec {
    static final String DEAD_LETTER_TOPIC = "product-stream-dead-letter"
    static final String TOPIC = "product-stream-test"
    static final String APP_ERROR_TOPIC = "app-event-error-unhandled"
    static final Collection<EventRecord> deadLetterEventRecords = new ConcurrentLinkedDeque<>()

    @Shared
    KafkaEventPublisher kafkaEventPublisher

    @Shared
    DeadLetterEventHandler deadLetterEventHandler

    void setupSpec() {
        kafkaEventPublisher = context.getBean(KafkaEventPublisher)
        deadLetterEventHandler = context.getBean(DeadLetterEventHandler)
    }

    void "event will eventually DLQ if there is no processor found for it"() {
        given:
        ProductDeactivated productDeactivated = new ProductDeactivated(UUID.randomUUID())
        EventRecord eventRecord = new EventRecord(UUID.randomUUID(), "test-app", productDeactivated)

        when:
        kafkaEventPublisher.publish(productDeactivated.productId(), eventRecord)

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> deadLetterEventRecords.size() > 0)

        then:
        EventRecord deadLetterEventRecord = deadLetterEventRecords.iterator().next()
        deadLetterEventRecord != null
        deadLetterEventRecord.exceptionSummary() != null
        deadLetterEventRecord.exceptionSummary().message()
                .contains("no processor found for event ProductDeactivated, moving on.")

        and:
        Class<?> eventClass = deadLetterEventRecord.eventClass()
        eventClass == ProductDeactivated

        and:
        Event event = deadLetterEventRecord.getEvent(eventClass)
        event != null
        event.data == productDeactivated
    }

    void cleanup() {
        deadLetterEventRecords.clear()
    }

    @Primary
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Requires(property = "spec.name", value = "DeadLetterEventListenerSpec")
    static class DeadLetterEventHandler {

        @Topic(DEAD_LETTER_TOPIC)
        void onReceive(EventRecord eventRecord) {
            deadLetterEventRecords.add(eventRecord)
        }
    }

    @Override
    Map<String, Object> getAdditionalProperties() {
        return [
                "app.event.topic"             : TOPIC,
                "app.event.retry.topic"       : "$TOPIC-retry",
                "app.event.dead-letter.topic" : DEAD_LETTER_TOPIC,
                "app.event.error.topic"       : APP_ERROR_TOPIC,
                "should-dead-letter-unhandled": "true"
        ]
    }

    @Override
    String getSpecName() {
        return "DeadLetterEventListenerSpec"
    }
}