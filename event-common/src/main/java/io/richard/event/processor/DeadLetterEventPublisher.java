package io.richard.event.processor;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventRecord;
import java.util.UUID;

public interface DeadLetterEventPublisher {

    <T> void handle(T event, UUID correlationId, String partitionKey);

    void handle(EventRecord eventRecord);
}
