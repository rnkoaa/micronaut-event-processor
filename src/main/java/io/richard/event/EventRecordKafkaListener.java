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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD;

@KafkaListener(groupId = "${app.event.groupId}", offsetReset = OffsetReset.EARLIEST, errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD, retryDelay = "50ms", retryCount = 0))
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
                headers.put("correlation-id", correlationId.toString());
            }
        }

        if (!headers.containsKey("ce-trace-id")) {
            headers.put("ce-trace-id", UUID.randomUUID().toString());
        }

        if (!headers.containsKey("correlation-id")) {
            headers.put("correlation-id", correlationId.toString());
        }
        return headers;
    }

    @Topic("${app.event.retry.topic}")
    void consumeRetryEvents(ConsumerRecord<String, EventRecord> consumerRecord,
        Consumer<String, byte[]> kafkaConsumer) {
        EventRecord eventRecord = consumerRecord.value();
        Map<String, Object> headers = extractHeaders(consumerRecord);
        String correlationId = (String) headers.getOrDefault("correlation-id", UUID.randomUUID().toString());
        EventMetadata metadata = enrichMetadata(consumerRecord.topic(), correlationId, eventRecord.metadata());
        var finalEventRecord = eventRecord.withHeaders(headers);
        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
        event = event.withCorrelationId(UUID.fromString(correlationId));

        // TODO before we process the retry message, we should ensure that the next retry message has been met
        try {
            eventProcessorGroup.processEvent(event, metadata);
        } catch (DeadLetterException ex) {
            metadata = metadata.withDead(true);
            eventRecord = eventRecord.withMetadata(metadata);
            eventRecord = eventRecord.withException(new ExceptionSummary(ex));
            deadLetterEventPublisher.handle(eventRecord);
        } catch (RetryableException ex) {
            RetryPolicy retry = metadata.retry();
            if (retry == null) {
                retry = new RetryPolicy();
            }
            retry = retry.incrementRetryCounter();
            var retryEventRecord = finalEventRecord.withMetadata(metadata.withRetry(retry));
            if (retry.retryCounter() > retry.maxRetry()) {
                // we've exceeded our retry counts, deadletter
                deadLetterEventPublisher.handle(retryEventRecord);
                return;
            }

            Headers kafkaHeaders = new RecordHeaders();
            headers.forEach((key, value) -> kafkaHeaders.add(
                new RecordHeader(key, ((String) value).getBytes(Charset.defaultCharset()))));

            kafkaEventPublisher.publishRetry(event.getPartitionKey(), kafkaHeaders, retryEventRecord);
        } catch (Exception ex) {
            System.out.println("Got an unhandled exception " + ex.getMessage());
        }

        kafkaConsumer.commitSync(
            Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")));
    }

    @Topic("${app.event.topic}")
    void consumeEvents(ConsumerRecord<String, EventRecord> consumerRecord, Consumer<String, byte[]>
        kafkaConsumer) {
        EventRecord eventRecord = consumerRecord.value();
        Map<String, Object> headers = extractHeaders(consumerRecord);
        String correlationId = (String) headers.getOrDefault("correlation-id", UUID.randomUUID().toString());
        EventMetadata metadata = enrichMetadata(consumerRecord.topic(), correlationId, eventRecord.metadata());
        var finalEventRecord = eventRecord.withHeaders(headers);
        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
        event = event.withCorrelationId(UUID.fromString(correlationId));

        try {
            eventProcessorGroup.processEvent(event, metadata);
        } catch (DeadLetterException ex) {
            metadata = metadata.withDead(true);
            eventRecord = eventRecord.withMetadata(metadata);
            eventRecord = eventRecord.withException(new ExceptionSummary(ex));
            deadLetterEventPublisher.handle(eventRecord);
        } catch (RetryableException ex) {
            var retry = new RetryPolicy();
            var retryEventRecord = finalEventRecord.withMetadata(metadata.withRetry(retry));

            Headers kafkaHeaders = new RecordHeaders();
            headers.forEach((key, value) -> kafkaHeaders.add(
                new RecordHeader(key, ((String) value).getBytes(Charset.defaultCharset()))));

            kafkaEventPublisher.publishRetry(event.getPartitionKey(), kafkaHeaders, retryEventRecord);
        } catch (Exception ex) {
            System.out.println("Got an unhandled exception");

        }

        kafkaConsumer.commitSync(
            Collections.singletonMap(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")));
    }

    private EventMetadata enrichMetadata(String topic, String correlationId, EventMetadata eventMetadata) {
        return eventMetadata.copy()
            .withSourceTopic(topic)
            .withCorrelationId(UUID.fromString(correlationId));
    }
}
