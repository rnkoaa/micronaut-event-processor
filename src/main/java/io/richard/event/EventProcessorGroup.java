package io.richard.event;

import java.util.HashMap;

public class EventProcessorGroup extends HashMap<Class<?>, Object> {

    @SuppressWarnings("unchecked")
    <T> void processEvent(Event<T> event) {
        EventProcessor<T> eventProcessor = (EventProcessor<T>) get(event.getEventType());
        if (eventProcessor == null) {
            return;
        }

        eventProcessor.process(event.getData());
    }
}
