package io.richard.event;

import jakarta.inject.Singleton;

@Singleton
@KafkaEventProcessor(ProductUpdatedEvent.class)
public class ProductUpdatedEventProcessor implements EventProcessor<ProductUpdatedEvent> {

    @Override
    public void process(ProductUpdatedEvent event) {
        System.out.println("processing ProductUpdatedEvent " + event);
    }
}
