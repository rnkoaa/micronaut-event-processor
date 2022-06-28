package io.richard.event;

import io.richard.event.annotations.KafkaEventProcessor;
import io.richard.event.processor.EventProcessor;
import jakarta.inject.Singleton;

@Singleton
@KafkaEventProcessor(ProductCreatedEvent.class)
public class ProductCreatedEventProcessor implements EventProcessor<ProductCreatedEvent> {

    @Override
    public void process(ProductCreatedEvent event) {
        System.out.println("Processing event " + event);
    }
}
