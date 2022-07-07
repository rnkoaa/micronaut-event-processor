package io.richard.event.processor;

import io.richard.event.annotations.Event;

public interface DeadLetterEventPublisher {

    <T> void handle(Event<T> event);
}
