package io.richard.event;

import io.micronaut.context.annotation.Factory;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.config.ApplicationEventConfig;
import io.richard.event.processor.DeadLetterEventPublisher;
import io.richard.event.processor.EventProcessorConfig;
import io.richard.event.processor.EventProcessorGroupImpl;
import jakarta.inject.Singleton;

@Factory
public class EventFactory {

    @Singleton
    public EventProcessorGroup provideEventProcessorGroup(
        DeadLetterEventPublisher deadLetterEventPublisher,
        ApplicationEventConfig config)  {
        var eventProcessorConfig = new EventProcessorConfig(config.shouldDeadLetterUnhandled());
        return new EventProcessorGroupImpl(deadLetterEventPublisher, eventProcessorConfig);
    }
}
