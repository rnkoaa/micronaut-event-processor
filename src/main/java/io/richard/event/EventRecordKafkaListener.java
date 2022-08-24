package io.richard.event;

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.ErrorContext;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.ExceptionSummary;
import io.richard.event.error.DeadLetterException;
import io.richard.event.error.RetryableException;
import io.richard.event.processor.DeadLetterEventPublisher;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaListener(groupId = "${app.event.groupId}", offsetReset = OffsetReset.EARLIEST, errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD, retryDelay = "50ms", retryCount = 0))
public class EventRecordKafkaListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRecordKafkaListener.class);

    private final EventProcessorGroup eventProcessorGroup;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final DeadLetterEventPublisher deadLetterEventPublisher;
    private final ObjectMapper objectMapper;

    public EventRecordKafkaListener(
        ObjectMapper objectMapper,
        KafkaEventPublisher kafkaEventPublisher,
        DeadLetterEventPublisher deadLetterEventPublisher,
        EventProcessorGroup eventProcessorGroup) {
        this.objectMapper = objectMapper;
        this.eventProcessorGroup = eventProcessorGroup;
        this.kafkaEventPublisher = kafkaEventPublisher;
        this.deadLetterEventPublisher = deadLetterEventPublisher;
    }

    @Topic("${app.event.retry.topic}")
    void consumeRetryEvents(ConsumerRecord<String, byte[]> consumerRecord,
        Consumer<String, byte[]> kafkaConsumer) {
        Map<String, Object> headers = KafkaRecordHeaders.extractHeaders(consumerRecord);
        var eventMetadata = KafkaRecordHeaders.fromKafkaEventHeaders(headers);
        try {
            Class<?> objectType = findEventClass(eventMetadata);

            Object kafkaRecord = deserializeMessage(eventMetadata.version(), consumerRecord.value(), objectType);
        } catch (DeadLetterException ex) {
            var deadMetadata = eventMetadata.withRetry(
                eventMetadata.retry().markDead()
            );

            var errorContext = buildErrorContext(consumerRecord, ex);
            var deadLetterEventRecord = new DeadLetterEventRecord(errorContext, deadMetadata);

//            var eventRecord = new EventRecord(String.format("""
//                {
//                    "message": "%s"
//                }
//                """, ex.getMessage()),
//                eventMetadata.correlationId(), eventMetadata.partitionKey()); 
//            metadata = metadata.withDead(true);
//            eventRecord = eventRecord.withMetadata(metadata);
//            eventRecord = eventRecord.withException(new ExceptionSummary(ex));
            deadLetterEventPublisher.handle(deadLetterEventRecord);
        }
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

    private static ErrorContext buildErrorContext(ConsumerRecord<String, byte[]> consumerRecord,
        DeadLetterException ex) {
        return new ErrorContext(
            consumerRecord.topic(),
            null,
            Instant.now(),
            consumerRecord.offset(),
            consumerRecord.key(),
            consumerRecord.partition(),
            new ExceptionSummary(ex)
        );
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

    public Class<?> findEventClass(EventMetadata eventMetadata) {
        try {
            return Class.forName(eventMetadata.objectType());
        } catch (ClassNotFoundException e) {
            throw new DeadLetterException(
                "Object Type " + eventMetadata.objectType() + " does not exist on the classpath");
        }
    }

    // any object that is version 0 will we wrapped in Event<T> class but version 2 and up will not be,
    public Object deserializeMessage(int version, byte[] message, Class<?> objectType) {
        if (version > 1) {
            try {
                return objectMapper.readValue(message, objectType);
            } catch (IOException e) {
                throw new DeadLetterException(
                    "unable to deserialize message for type " + objectType.getSimpleName());
            }
        }
        return null;
    }
}
