package io.richard.event.annotations;

import jakarta.annotation.Nonnull;

import java.util.Map;

public interface EventProcessorGroup extends Map<Class<?>, Object> {

    <T> void processEvent(@Nonnull Event<T> event, @Nonnull EventMetadata eventMetadata);
}
