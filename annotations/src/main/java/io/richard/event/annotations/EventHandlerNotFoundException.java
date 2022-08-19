package io.richard.event.annotations;

public class EventHandlerNotFoundException extends RuntimeException {

    private final Class<?> dataClass;

    public EventHandlerNotFoundException(Class<?> dataClass) {
        super("No Handler found for class %s " + dataClass.getCanonicalName());
        this.dataClass = dataClass;
    }

    public Class<?> getDataClass() {
        return dataClass;
    }
}