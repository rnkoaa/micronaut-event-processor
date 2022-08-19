package io.richard.event.annotations;

@FunctionalInterface
public interface ProcessorProxy {

    void handle(EventRecord eventRecord);
}