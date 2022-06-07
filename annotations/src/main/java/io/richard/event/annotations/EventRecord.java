package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record EventRecord(
    UUID id,
    String source,
    String type,
    Instant timestamp,

    @JsonProperty("simple_class_name")
    String simpleClassName,
    EventMetadata metadata,

    @JsonIgnore
    Class<?> eventClass,

    @JsonIgnore
    byte[] rawMessage,

    Object data
) {

    public EventRecord {
        Objects.requireNonNull(data);

        if (timestamp == null) {
            timestamp = Instant.now();
        }
        if (metadata == null) {
            throw new IllegalArgumentException("timestamp cannot be null");
        }
        if (simpleClassName == null || simpleClassName.isEmpty()) {
            simpleClassName = type.split("\\.")[type.length() - 1];
        }
    }

    private EventRecord(Object data) {
        this(UUID.randomUUID(), System.getenv("USER"), data.getClass().getCanonicalName(), Instant.now(),
            data.getClass().getSimpleName(), new EventMetadata(), data.getClass(),
            null, data);
    }
    public EventRecord(UUID id, String source, Object data, EventMetadata metadata) {
        this(id, source, data.getClass().getCanonicalName(), Instant.now(),
            data.getClass().getSimpleName(), metadata, data.getClass(),
            null, data);
    }

    public EventRecord withRawMessage(byte[] rawMessage) {
        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data);
    }

    public EventRecord withData(Object data) {
        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data);
    }

    public <T> T getTypedData(Class<T> typedClass) {
        return typedClass.cast(data);
    }
}
