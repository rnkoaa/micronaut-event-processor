package io.richard.event.annotations;

import java.util.Map;

public interface EventProcessorGroup extends Map<Class<?>, Object> {

    <T> void processEvent(Event<T> event);
}
