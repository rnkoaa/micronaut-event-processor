package io.richard.event.error;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Replaces;
import io.richard.event.AbstractkafkaTest;
import io.richard.event.KafkaEventPublisher;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class EventRecordKafkaExceptionHandlerTest extends AbstractkafkaTest {
    private static final String ORDER_STREAM_TOPIC = "app-product-stream-test";
    private static final String APP_EVENT_DEAD_LETTER = "app-event-dead-letter";
    private static final String APP_ERROR_TOPIC = "app-event-error-unhandled";

    private static final Collection<DeadLetterEventRecord> deadLetterEventRecords = new ConcurrentLinkedDeque<>();

    @Inject
    BadMessageProducer badMessageProducer;

    @Inject
    ErrorHandlerConsumer errorHandlerConsumer;

    @Inject
    ObjectMapper objectMapper;

    @Override
    protected Map<String, String> additionalProperties() {
        return Map.of(
            "product.stream.topic", ORDER_STREAM_TOPIC,
            "app.event.dead-letter", APP_EVENT_DEAD_LETTER,
            "app.error.topic", APP_ERROR_TOPIC
        );
    }

    @AfterEach
    void afterEach() {
        deadLetterEventRecords.clear();
    }

    @Test
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

    @KafkaClient
    @Replaces(KafkaEventPublisher.class)
    interface BadMessageProducer extends KafkaEventPublisher {

        @Topic(ORDER_STREAM_TOPIC)
        void publish(Map<String, Object> event);

    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class ErrorHandlerConsumer {

        @Topic(APP_ERROR_TOPIC)
        void receive(DeadLetterEventRecord deadLetterEventRecord) {
            deadLetterEventRecords.add(deadLetterEventRecord);
        }
    }
}