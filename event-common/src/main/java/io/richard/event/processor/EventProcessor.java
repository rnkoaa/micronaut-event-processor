package io.richard.event.processor;

public interface EventProcessor<T> {

    void process(T object);
}
