package io.richard.event;

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_CLOUD_EVENT_TRACE_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_CONTENT_TYPE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_CORRELATION_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_EVENT_PRIORITY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_EVENT_TIMESTAMP;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_EVENT_VERSION;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_IS_EVENT_DEAD;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_MAX_RETRY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_NEXT_RETRY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_OBJECT_TYPE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_PARENT_ID;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_PARTITION_KEY;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_PUBLISHED_TIMESTAMP;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_RETRY_COUNT;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_SIMPLE_OBJECT_TYPE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_SOURCE;
import static io.richard.event.KafkaEventHeaderKeys.KAFKA_SOURCE_TOPIC;
import static io.richard.event.annotations.EventRecordHeaders.CONTENT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.CORRELATION_ID;
import static io.richard.event.annotations.EventRecordHeaders.EVENT_PRIORITY;
import static io.richard.event.annotations.EventRecordHeaders.EVENT_TIMESTAMP;
import static io.richard.event.annotations.EventRecordHeaders.EVENT_VERSION;
import static io.richard.event.annotations.EventRecordHeaders.ID;
import static io.richard.event.annotations.EventRecordHeaders.IS_EVENT_DEAD;
import static io.richard.event.annotations.EventRecordHeaders.MAX_RETRY;
import static io.richard.event.annotations.EventRecordHeaders.NEXT_RETRY;
import static io.richard.event.annotations.EventRecordHeaders.OBJECT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.PARENT_ID;
import static io.richard.event.annotations.EventRecordHeaders.PARTITION_KEY;
import static io.richard.event.annotations.EventRecordHeaders.PUBLISHED_TIMESTAMP;
import static io.richard.event.annotations.EventRecordHeaders.RETRY_COUNT;
import static io.richard.event.annotations.EventRecordHeaders.SIMPLE_OBJECT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.SOURCE;
import static io.richard.event.annotations.EventRecordHeaders.SOURCE_TOPIC;

import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import io.richard.event.error.DeadLetterException;
import io.richard.event.error.RetryableException;
import io.richard.event.processor.DeadLetterEventPublisher;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    Map<String, Object> extractHeaders(ConsumerRecord<String, byte[]> consumerRecord) {
        /*

    @JsonProperty("max_retry")
    int maxRetry,

    @JsonProperty("next_retry")
    Instant nextRetry

         */
        Map<String, Object> headers = new HashMap<>();
        for (Header next : consumerRecord.headers()) {
            if (next.key().equals(KAFKA_CLOUD_EVENT_TRACE_ID)) {
                headers.put(CORRELATION_ID, UUID.nameUUIDFromBytes(next.value()));
            }
            if (next.key().equals(KAFKA_CORRELATION_ID)) {
                headers.put(CORRELATION_ID, UUID.nameUUIDFromBytes(next.value()));
            }
            if (next.key().equals(KAFKA_PARENT_ID)) {
                headers.put(PARENT_ID, UUID.nameUUIDFromBytes(next.value()));
            }
            if (next.key().equals(KAFKA_IS_EVENT_DEAD)) {
                headers.put(IS_EVENT_DEAD, Boolean.parseBoolean(new String(next.value())));
            }
            if (next.key().equals(KAFKA_SOURCE_TOPIC)) {
                headers.put(SOURCE_TOPIC, new String(next.value()));
            }
            if (next.key().equals(KAFKA_ID)) {
                headers.put(ID, new String(next.value()));
            }
            if (next.key().equals(KAFKA_CONTENT_TYPE)) {
                headers.put(CONTENT_TYPE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_EVENT_TIMESTAMP)) {
                headers.put(EVENT_TIMESTAMP, Instant.parse(new String(next.value())));
            }
            if (next.key().equals(KAFKA_PUBLISHED_TIMESTAMP)) {
                headers.put(PUBLISHED_TIMESTAMP, Instant.parse(new String(next.value())));
            }

            if (next.key().equals(KAFKA_OBJECT_TYPE)) {
                headers.put(OBJECT_TYPE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_EVENT_VERSION)) {
                headers.put(EVENT_VERSION, Integer.valueOf(new String(next.value())));
            }
            if (next.key().equals(KAFKA_EVENT_PRIORITY)) {
                headers.put(EVENT_PRIORITY, Integer.valueOf(new String(next.value())));
            }
            if (next.key().equals(KAFKA_SOURCE)) {
                headers.put(SOURCE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_PARTITION_KEY)) {
                headers.put(PARTITION_KEY, new String(next.value()));
            }
            if (next.key().equals(KAFKA_SIMPLE_OBJECT_TYPE)) {
                headers.put(SIMPLE_OBJECT_TYPE, new String(next.value()));
            }
            if (next.key().equals(KAFKA_MAX_RETRY)) {
                headers.put(MAX_RETRY, Integer.parseInt(new String(next.value())));
            }
            if (next.key().equals(KAFKA_RETRY_COUNT)) {
                headers.put(RETRY_COUNT, Integer.parseInt(new String(next.value())));
            }
            if (next.key().equals(KAFKA_NEXT_RETRY)) {
                headers.put(NEXT_RETRY, Instant.parse(new String(next.value())));
            }
        }

        headers.putIfAbsent(EVENT_VERSION, 1);
        headers.putIfAbsent(RETRY_COUNT, 0);
        headers.putIfAbsent(MAX_RETRY, 3);
        headers.putIfAbsent(CONTENT_TYPE, "application/json");
        headers.putIfAbsent(CORRELATION_ID, UUID.randomUUID());
        headers.putIfAbsent(ID, UUID.randomUUID());
        headers.putIfAbsent(PUBLISHED_TIMESTAMP, Instant.now().toString());
        return headers;
    }

    @Topic("${app.event.retry.topic}")
    void consumeRetryEvents(ConsumerRecord<String, byte[]> consumerRecord,
        Consumer<String, byte[]> kafkaConsumer) {
        var eventData = consumerRecord.value();
        Map<String, Object> headers = extractHeaders(consumerRecord);
//        String correlationId = (String) headers.getOrDefault("correlation-id", UUID.randomUUID().toString());
//        EventMetadata metadata = enrichMetadata(consumerRecord.topic(), correlationId, eventRecord.metadata());
//        var finalEventRecord = eventRecord.withHeaders(headers);
//        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
//        event = event.withCorrelationId(UUID.fromString(correlationId));

        // TODO before we process the retry message, we should ensure that the next retry message has been met
        try {
//            eventProcessorGroup.processEvent(event, metadata);
        } catch (DeadLetterException ex) {
//            metadata = metadata.withDead(true);
//            eventRecord = eventRecord.withMetadata(metadata);
//            eventRecord = eventRecord.withException(new ExceptionSummary(ex));
//            deadLetterEventPublisher.handle(eventRecord);
        } catch (RetryableException ex) {
//            RetryPolicy retry = metadata.retry();
//            if (retry == null) {
//                retry = new RetryPolicy();
//            }
//            retry = retry.incrementRetryCounter();
//            var retryEventRecord = finalEventRecord.withMetadata(metadata.withRetry(retry));
//            if (retry.retryCounter() >= retry.maxRetry()) {
//                LOGGER.debug("Exceeded max retry count of {}, dead lettering event with id {}",
//                    retry.maxRetry(), eventRecord.id());
//                // we've exceeded our retry counts, dead letter
//                deadLetterEventPublisher.handle(retryEventRecord);
//                return;
//            }
//
//            Headers kafkaHeaders = new RecordHeaders();
//            headers.forEach((key, value) -> kafkaHeaders.add(
//                new RecordHeader(key, ((String) value).getBytes(Charset.defaultCharset()))));
//
//            kafkaEventPublisher.publishRetry(event.getPartitionKey(), retryEventRecord, kafkaHeaders);
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
       /* EventRecord eventRecord = consumerRecord.value();
        Map<String, Object> headers = extractHeaders(consumerRecord);
        String correlationId = (String) headers.getOrDefault("correlation-id", UUID.randomUUID().toString());
        EventMetadata metadata = enrichMetadata(consumerRecord.topic(), correlationId, eventRecord.metadata());
        var finalEventRecord = eventRecord.withHeaders(headers);
        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
        event = event.withCorrelationId(UUID.fromString(correlationId));*/

        try {
//            eventProcessorGroup.processEvent(event, metadata);
        } catch (DeadLetterException ex) {
           /* metadata = metadata.withDead(true);
            eventRecord = eventRecord.withMetadata(metadata);
            eventRecord = eventRecord.withException(new ExceptionSummary(ex));
            deadLetterEventPublisher.handle(eventRecord);*/
        } catch (RetryableException ex) {
           /* var retry = new RetryPolicy();
            var retryEventRecord = finalEventRecord.withMetadata(metadata.withRetry(retry));

            Headers kafkaHeaders = new RecordHeaders();
            headers.forEach((key, value) -> kafkaHeaders.add(
                new RecordHeader(key, ((String) value).getBytes(Charset.defaultCharset()))));

            kafkaEventPublisher.publishRetry(event.getPartitionKey(), retryEventRecord, kafkaHeaders);*/
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
