package io.richard.event;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import io.richard.event.processor.DeadLetterEventPublisher;
import jakarta.inject.Singleton;

@Singleton
public class DeadLetterEventPublisherImpl implements DeadLetterEventPublisher {

    private final KafkaEventPublisher kafkaEventPublisher;

    public DeadLetterEventPublisherImpl(
        KafkaEventPublisher kafkaEventPublisher) {
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @Override
    public <T> void handle(Event<T> event) {
        var deadEventMetadata = new EventMetadata().withDead(true)
            .withCorrelationId(event.getCorrelationId());
//        var eventRecord = new EventRecord(event.getId(), "product-service", event.getData(), deadEventMetadata);
//        kafkaEventPublisher.publishDeadLetter(event.getPartitionKey(), eventRecord);
    }

    @Override
    public void handle(EventRecord eventRecord) {
//        Event<?> event = eventRecord.getEvent(eventRecord.eventClass());
//        kafkaEventPublisher.publishDeadLetter(event.getPartitionKey(), eventRecord);
    }
}
