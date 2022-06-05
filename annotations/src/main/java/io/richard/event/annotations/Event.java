package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;

public class Event<T> {

    private final String type;
    private final String name;
    private final T data;

    @JsonIgnore
    private final Class<?> eventType;


    public Event(T data) {
        Objects.requireNonNull(data);
        this.data = data;
        this.eventType = data.getClass();
        this.type = eventType.getCanonicalName();
        this.name = eventType.getSimpleName();
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Class<?> getEventType() {
        return eventType;
    }

    public T getData() {
        return data;
    }
}
