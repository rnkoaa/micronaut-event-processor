package io.richard.event;

import io.richard.event.annotations.KafkaEventProcessor;
import io.richard.event.processor.EventProcessor;
import jakarta.inject.Singleton;

@Singleton
@KafkaEventProcessor(ProductCreatedEvent.class)
public class ProductCreatedEventProcessor implements EventProcessor<ProductCreatedEvent> {

    private final EventCollector eventCollector;

    public ProductCreatedEventProcessor(EventCollector eventCollector) {
        this.eventCollector = eventCollector;
    }

    @Override
    public void process(ProductCreatedEvent event) {
        System.out.println("Processing event " + event);
        eventCollector.add(event);
    }
}
