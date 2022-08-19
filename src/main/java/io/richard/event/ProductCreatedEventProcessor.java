package io.richard.event;

import io.richard.event.annotations.KafkaEventProcessor;
import io.richard.event.processor.EventProcessor;
import jakarta.inject.Singleton;

@Singleton
public class ProductCreatedEventProcessor implements EventProcessor<ProductCreatedEvent> {

    private final EventCollector eventCollector;

    public ProductCreatedEventProcessor(EventCollector eventCollector) {
        this.eventCollector = eventCollector;
    }

    @Override
    @KafkaEventProcessor
    public void process(ProductCreatedEvent event) {
        System.out.println("Processing event " + event);
        eventCollector.add(event);
    }
}
