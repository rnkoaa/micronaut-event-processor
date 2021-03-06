package io.richard.event.processor;

import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.ExceptionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class EventProcessorGroupImpl extends HashMap<Class<?>, Object> implements EventProcessorGroup {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessorGroupImpl.class);

    private final EventProcessorConfig eventProcessorConfig;
    private final DeadLetterEventPublisher deadLetterEventPublisher;

    public EventProcessorGroupImpl(DeadLetterEventPublisher deadLetterEventPublisher,
                                   EventProcessorConfig eventProcessorConfig) {
        this.eventProcessorConfig = eventProcessorConfig;
        this.deadLetterEventPublisher = deadLetterEventPublisher;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void processEvent(Event<T> event, EventMetadata eventMetadata) {
        EventProcessor<T> eventProcessor = (EventProcessor<T>) get(event.getEventType());
        String exceptionMessage = String.format("no processor found for event %s, moving on.", event.getEventType().getSimpleName());
        if (eventProcessor == null) {
            logger.warn(exceptionMessage);

            if (eventProcessorConfig.shouldDeadLetterUnhandled()) {

                EventRecord eventRecord = new EventRecord(
                    event.getId(), eventMetadata.sourceTopic(), event.getType(),
                    event.getTimestamp(), event.getName(), eventMetadata, event.getEventType(),
                    null, event.getData(), Map.of(), new ExceptionSummary(exceptionMessage)
                );
                deadLetterEventPublisher.handle(eventRecord);
            }

            return;
        }

        eventProcessor.process(event.getData());
    }
}
