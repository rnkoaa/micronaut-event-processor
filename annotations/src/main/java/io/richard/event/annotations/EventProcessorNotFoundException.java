package io.richard.event.annotations;

public class EventProcessorNotFoundException extends RuntimeException {

    private final Class<?> dataClass;

    public EventProcessorNotFoundException(Class<?> dataClass) {
        super("No processor defined for class %s " + dataClass.getCanonicalName());
        this.dataClass = dataClass;
    }

    public Class<?> getDataClass() {
        return dataClass;
    }
}