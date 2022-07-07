package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
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

    Object data,

    Map<String, Object> headers
) {

    public EventRecord {
        Objects.requireNonNull(data);
        Objects.requireNonNull(headers);

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
            null, data, new HashMap<>());
    }

    public EventRecord(UUID id, String source, Object data, EventMetadata metadata) {
        this(id, source, data.getClass().getCanonicalName(), Instant.now(),
            data.getClass().getSimpleName(), metadata, data.getClass(),
            null, data, new HashMap<>());
    }

    public EventRecord withRawMessage(byte[] rawMessage) {
        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
            headers);
    }

    public EventRecord withData(Object data) {
        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
            headers);
    }

    public EventRecord withHeaders(Map<String, Object> headers) {
        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
            headers);
    }

    public <T> T getTypedData(Class<T> typedClass) {
        return typedClass.cast(data);
    }

    public <T> Event<T> getEvent(Class<T> typedClass) {
        //add other details here for the event
        UUID partitionKey = (UUID) headers.getOrDefault("partitionKey", UUID.randomUUID());
        String messageKey = (String) headers.getOrDefault("messageKey", "");
        var event = new Event<>(id, timestamp, partitionKey, messageKey, typedClass.cast(data));
        return event.withCorrelationId(metadata.correlationId());
    }

    public EventRecord copy() {
        var headerCopy = new HashMap<>(headers);

        return new EventRecord(
            id,
            source,
            type,
            timestamp,
            simpleClassName,
            metadata.copy(),
            eventClass,
            rawMessage,
            data,
            headerCopy
        );
    }

    public EventRecord withMetadata(EventMetadata eventMetadata) {
        return new EventRecord(id, source, type, timestamp, simpleClassName, eventMetadata, eventClass, rawMessage, data,
            headers);
    }
}
