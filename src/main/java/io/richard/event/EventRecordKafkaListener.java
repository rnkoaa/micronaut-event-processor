package io.richard.event;

import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.ExceptionSummary;
import io.richard.event.annotations.RetryPolicy;
import io.richard.event.error.DeadLetterException;
import io.richard.event.error.RetryableException;
import io.richard.event.processor.DeadLetterEventPublisher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD;

@KafkaListener(groupId = "${app.event.groupId}", offsetReset = OffsetReset.EARLIEST, errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD, retryDelay = "50ms", retryCount = 1))
public class EventRecordKafkaListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventRecordKafkaListener.class);

    private final EventProcessorGroup eventProcessorGroup;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final DeadLetterEventPublisher deadLetterEventPublisher;

    public EventRecordKafkaListener(KafkaEventPublisher kafkaEventPublisher,
                                    DeadLetterEventPublisher deadLetterEventPublisher,
                                    EventProcessorGroup eventProcessorGroup) {
        this.eventProcessorGroup = eventProcessorGroup;
        this.kafkaEventPublisher = kafkaEventPublisher;
        this.deadLetterEventPublisher = deadLetterEventPublisher;
    }

    Map<String, Object> extractHeaders(ConsumerRecord<String, EventRecord> consumerRecord) {
        Map<String, Object> headers = new HashMap<>();
        UUID correlationId = UUID.randomUUID();
        for (Header next : consumerRecord.headers()) {
            if (next.key().equals("ce-trace-id")) {
                headers.put("ce-trace-id", new String(next.value()));
            }
            if (next.key().equals("correlation-id")) {
                correlationId = UUID.fromString(new String(next.value()));
                headers.put("correlation-id", correlationId);
            }
        }

        if (!headers.containsKey("ce-trace-id")) {
            headers.put("ce-trace-id", UUID.randomUUID().toString());
        }

        if (!headers.containsKey("correlation-id")) {
            headers.put("correlation-id", correlationId);
        }
        return headers;
    }

    @Topic("${app.event.topic}")
    void consumeEvents(ConsumerRecord<String, EventRecord> consumerRecord, Consumer<String, byte[]> kafkaConsumer) {
        EventRecord eventRecord = consumerRecord.value();
        Map<String, Object> headers = extractHeaders(consumerRecord);
        UUID correlationId = (UUID) headers.getOrDefault("correlation-id", UUID.randomUUID());
        EventMetadata metadata = enrichMetadata(consumerRecord.topic(), correlationId, eventRecord.metadata());
        var finalEventRecord = eventRecord.withHeaders(headers);
        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
        event = event.withCorrelationId(correlationId);

        try {
            eventProcessorGroup.processEvent(event, metadata);
        } catch (DeadLetterException ex) {
            metadata = metadata.withDead(true);
            eventRecord = eventRecord.withMetadata(metadata);
            eventRecord = eventRecord.withException(new ExceptionSummary(ex));
            deadLetterEventPublisher.handle(eventRecord);
        } catch (Exception ex) {
            if (ex instanceof RetryableException retryableException) {
// send to retry topic for topic
//                eventRecord
                var retry = new RetryPolicy();
                var metadataWithRetry = metadata.withRetry(retry);
                var retryEventRecord = new EventRecord(event.getId(), "product-service", event.getData(), metadataWithRetry);

                kafkaEventPublisher.publishRetry(event.getPartitionKey(), retryEventRecord);
            } else {
                // publish to dead letter
            }
        }

        kafkaConsumer.commitSync(
            Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")));
    }

    private EventMetadata enrichMetadata(String topic, UUID correlationId, EventMetadata eventMetadata) {
        return eventMetadata.copy()
            .withSourceTopic(topic)
            .withCorrelationId(correlationId);
    }
}
