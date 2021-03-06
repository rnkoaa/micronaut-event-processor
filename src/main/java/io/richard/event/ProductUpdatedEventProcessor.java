package io.richard.event;

import io.richard.event.annotations.KafkaEventProcessor;
import io.richard.event.processor.EventProcessor;
import jakarta.inject.Singleton;

@Singleton
@KafkaEventProcessor(ProductUpdatedEvent.class)
public class ProductUpdatedEventProcessor implements EventProcessor<ProductUpdatedEvent> {
    private final EventCollector eventCollector;

    public ProductUpdatedEventProcessor(EventCollector eventCollector) {
        this.eventCollector = eventCollector;
    }

    @Override
    public void process(ProductUpdatedEvent event) {
        System.out.println("processing ProductUpdatedEvent " + event);
        eventCollector.add(event);
    }
}
