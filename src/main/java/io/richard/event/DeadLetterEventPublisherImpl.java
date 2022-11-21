package io.richard.event;

import static io.richard.event.KafkaRecordHeaders.buildHeaders;

import io.richard.event.annotations.ErrorContext;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.RetryPolicy;
import io.richard.event.processor.DeadLetterEventPublisher;
import jakarta.inject.Singleton;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

@Singleton
public class DeadLetterEventPublisherImpl implements DeadLetterEventPublisher {

    private final KafkaEventPublisher kafkaEventPublisher;

    public DeadLetterEventPublisherImpl(
        KafkaEventPublisher kafkaEventPublisher) {
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @Override
    public <T> void handle(T event, UUID correlationId, String partitionKey) {
        var deadEventMetadata = new EventMetadata(
            event.getClass(), correlationId, partitionKey
        ).withRetry(new RetryPolicy().markDead());

    }

    @Override
    public void handle(EventRecord eventRecord) {
//        Event<?> event = eventRecord.getEvent(eventRecord.eventClass());
//        kafkaEventPublisher.publishDeadLetter(event.getPartitionKey(), eventRecord);
    }

    @Override
    public void handle(DeadLetterEventRecord eventRecord) {
        // set headers and publish just the data
        Headers kafkaHeaders = convert(buildHeaders(eventRecord.id(), eventRecord.metadata()));
        kafkaEventPublisher.publishDeadLetter(eventRecord.metadata().partitionKey(), eventRecord.data(), kafkaHeaders);
    }

    Headers convert(Map<String, Object> headers) {
        List<Header> kafkaHeaders = headers.entrySet()
            .stream()
            .map(it -> new RecordHeader(it.getKey(), getBytes(it)))
            .map(it -> (Header) it)
            .toList();

        return new RecordHeaders(kafkaHeaders);

    }

    private static byte[] getBytes(Entry<String, Object> it) {
        if (it.getValue() instanceof String s) {
            return s.getBytes(Charset.defaultCharset());
        }
        if (it.getValue() instanceof UUID id) {
            return id.toString().getBytes(Charset.defaultCharset());
        }
        if (it.getValue() instanceof Instant instant) {
            return instant.toString().getBytes(Charset.defaultCharset());
        }
        if(it.getValue() instanceof Integer value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
        if(it.getValue() instanceof Boolean value) {
            return new byte[] { value ? (byte) 1: (byte) 0};
        }
        return null;
    }

    Map<String, Object> deadEvent(ErrorContext errorContext) {
        return Map.of(
            "created_At", errorContext.createdAt(),
            "message_key", errorContext.messageKey(),
            "partition", errorContext.partition(),
            "offset", errorContext.offset(),
            "error_summary", errorContext.summary().message()

        );
    }
}
