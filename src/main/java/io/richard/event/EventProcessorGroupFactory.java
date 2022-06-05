package io.richard.event;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class EventProcessorGroupFactory {

    private final ApplicationContext applicationContext;

    public EventProcessorGroupFactory(ApplicationContext context) {
        this.applicationContext = context;
    }

    @Singleton
    public EventProcessorGroup eventProcessorGroup() {
        EventProcessorGroup eventProcessorGroup = new EventProcessorGroup();
        eventProcessorGroup.put(ProductCreatedEvent.class,
            applicationContext.getBean(ProductCreatedEventProcessor.class));
        eventProcessorGroup.put(ProductUpdatedEvent.class,
            applicationContext.getBean(ProductUpdatedEventProcessor.class));

        return eventProcessorGroup;
    }
}
