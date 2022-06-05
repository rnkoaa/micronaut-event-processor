package io.richard.event;

public interface EventProcessor<T> {

    void process(T object);
}
