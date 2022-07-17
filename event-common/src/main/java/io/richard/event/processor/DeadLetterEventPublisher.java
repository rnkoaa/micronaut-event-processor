package io.richard.event.processor;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventRecord;

public interface DeadLetterEventPublisher {

    <T> void handle(Event<T> event);

    void handle(EventRecord eventRecord);
}
