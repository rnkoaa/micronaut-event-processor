package io.richard.event.processor;

public record EventProcessorConfig(
    boolean shouldDeadLetterUnhandled
) {

}
