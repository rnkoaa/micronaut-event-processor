package io.richard.event


import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import io.richard.event.error.DeadLetterEventRecord
import spock.lang.Shared

import java.util.concurrent.ConcurrentLinkedDeque

class EventRecordKafkaExceptionHandlerSpec extends AbstractKafkaSpec {
    private static final String ORDER_STREAM_TOPIC = "app-product-stream-test"
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter"
    private static final String APP_ERROR_TOPIC = "app-event-error-unhandled"

    private static final Collection<DeadLetterEventRecord> deadLetterEventRecords = new ConcurrentLinkedDeque<>()

    @Shared
    BadMessageProducer badMessageProducer

    @Shared
    ErrorHandlerConsumer errorHandlerConsumer

    void setupSpec() {
        badMessageProducer = context.getBean(BadMessageProducer)
        errorHandlerConsumer = context.getBean(ErrorHandlerConsumer)
    }


    @Override
    String getSpecName() {
        return "EventRecordKafkaExceptionHandlerSpec"
    }

    @Override
    Map<String, Object> getAdditionalProperties() {
        return [
                "app.event.topic"            : ORDER_STREAM_TOPIC,
                "app.event.dead-letter.topic": APP_EVENT_DEAD_LETTER,
                "app.event.error.topic"      : APP_ERROR_TOPIC
        ]
    }
/*

    void badMessagesWillBeUnRecovered() throws IOException {
        String eventMessage = """
            {
                "metadata": {
                    "correlation_id": "cf55b229-2606-4616-9b92-934e3476efd1",
                    "priority": 1,
                    "version": 1
                },
                "ce_type": "io.richard.event.ProductCreatedEvent",
                "ce_source": "product-service",
                "ce_simple_type": "ProductCreatedEvent",
                "ce_id": "0302e4d4-d1e5-4eb9-94b5-72d9e66fae10",
                "ce_timestamp": "2022-06-25T17:47:39.000Z"

            }
            """.stripIndent().trim();

        TypeReference<Map<String, Object>> tRef = new TypeReference<>() {
        };
        Map<String, Object> event = objectMapper.readValue(eventMessage, tRef);

        badMessageProducer.publish(event);

        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> deadLetterEventRecords.size() > 0);

        DeadLetterEventRecord next = deadLetterEventRecords.iterator().next();
        assertThat(next).isNotNull();
    }
     */


    void cleanup() {
        deadLetterEventRecords.clear();
    }

    @KafkaClient
    @Primary
    @Requires(property = "spec.name", value = "EventRecordKafkaExceptionHandlerSpec")
    interface BadMessageProducer extends KafkaEventPublisher {

        @Topic(ORDER_STREAM_TOPIC)
        void publish(Map<String, Object> event);

    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Primary
    @Requires(property = "spec.name", value = "EventRecordKafkaExceptionHandlerSpec")
    static class ErrorHandlerConsumer {

        @Topic(APP_ERROR_TOPIC)
        void receive(DeadLetterEventRecord deadLetterEventRecord) {
            deadLetterEventRecords.add(deadLetterEventRecord)
        }
    }

}