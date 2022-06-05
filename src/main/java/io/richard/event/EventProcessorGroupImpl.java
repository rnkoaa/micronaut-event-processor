package io.richard.event;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventProcessorGroup;
import jakarta.inject.Singleton;
import java.util.HashMap;

@Singleton
public class EventProcessorGroupImpl extends HashMap<Class<?>, Object> implements EventProcessorGroup {

    @Override
    @SuppressWarnings("unchecked")
    public <T> void processEvent(Event<T> event) {
        EventProcessor<T> eventProcessor = (EventProcessor<T>) get(event.getEventType());
        if (eventProcessor == null) {
            return;
        }

        eventProcessor.process(event.getData());
    }
}
