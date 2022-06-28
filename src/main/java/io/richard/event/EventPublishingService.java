package io.richard.event;

import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import jakarta.inject.Singleton;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Singleton
public class EventPublishingService {
    private final KafkaEventPublisher kafkaEventPublisher;

    public EventPublishingService(KafkaEventPublisher kafkaEventPublisher) {
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    //
//        Headers headers = consumerRecord.headers();
//        List<String> headersForKey = List.of("ce-source",
//            "ce-type", "ce-timestamp", "ce-id", "ce-trace-id", "ce-correlation-id");
//        Map<String, String> messageHeaders = streamFromIterator(headers.iterator())
//            .filter(it -> headersForKey.contains(it.key()))
//            .collect(Collectors.toMap(Header::key, it -> new String(it.value(), StandardCharsets.UTF_8)));

    /**
     *
     * @param topic
     * @param partitionKey
     * @param event
     */
    void publish(final String topic, final UUID partitionKey, Object event) {

        var eventMetadata = new EventMetadata();
        var eventRecord = new EventRecord(UUID.randomUUID(), "source", event, eventMetadata);

        UUID key = (partitionKey != null) ? partitionKey : UUID.randomUUID();

        // set up headers
        Collection<Header> headers = List.of(
            uuidHeader("correlation-id", eventMetadata.correlationId()),
            uuidHeader("ce-trace-id", eventMetadata.correlationId())
        );

        kafkaEventPublisher.publish(topic, key, headers, eventRecord);
    }

    private RecordHeader uuidHeader(String key, UUID value) {
        Objects.requireNonNull(value, "record value");
        return new RecordHeader(key, value.toString().getBytes(StandardCharsets.UTF_8));
    }
}
