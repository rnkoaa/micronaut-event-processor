package io.richard.event;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import io.richard.event.config.ApplicationEventConfig;
import io.richard.event.processor.DeadLetterEventPublisher;
import jakarta.inject.Singleton;

@Singleton
public class DeadLetterEventPublisherImpl implements DeadLetterEventPublisher {

    private final KafkaEventPublisher kafkaEventPublisher;

    private final String deadLetterTopic;

    public DeadLetterEventPublisherImpl(
        ApplicationEventConfig applicationEventConfig,
        KafkaEventPublisher kafkaEventPublisher) {
        this.kafkaEventPublisher = kafkaEventPublisher;
        this.deadLetterTopic = applicationEventConfig.getDeadLetterTopic();
    }

    @Override
    public <T> void handle(Event<T> event) {
        var deadEventMetadata = new EventMetadata().withDead(true)
            .withCorrelationId(event.getCorrelationId());
        var eventRecord = new EventRecord(event.getId(), "product-service", event, deadEventMetadata);
        kafkaEventPublisher.publish(deadLetterTopic, event.getPartitionKey(), eventRecord);

    }
}
