package io.richard.event;

import io.micronaut.context.annotation.Factory;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.processor.EventProcessorGroupImpl;
import jakarta.inject.Singleton;

@Factory
public class EventFactory {

    @Singleton
    public EventProcessorGroup provideEventProcessorGroup() {
        return new EventProcessorGroupImpl();
    }
}
