package io.richard.event;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.RetryPolicy;
import io.richard.event.processor.DeadLetterEventPublisher;
import jakarta.inject.Singleton;
import java.util.UUID;

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
}
