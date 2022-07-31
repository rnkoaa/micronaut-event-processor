package io.richard.event.annotations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class Event<T> {

    private final UUID id;
    private final Instant timestamp;
    private final String type;
    private final String name;
    private final T data;

    @JsonIgnore
    private final UUID partitionKey;

    private UUID correlationId;

    @JsonIgnore
    private final String messageKey;

    @JsonIgnore
    private final Class<?> eventType;

    public Event(UUID partitionKey, String messageKey, T data) {
        this(UUID.randomUUID(), Instant.now(), partitionKey, messageKey, data);
    }

    public Event(T data) {
        this(UUID.randomUUID(), Instant.now(), UUID.randomUUID(), "", data);
    }

    public Event(UUID id, Instant timestamp, UUID partitionKey, String messageKey, T data) {
        Objects.requireNonNull(data);
        this.id = (id == null) ? UUID.randomUUID() : id;
        this.timestamp = (timestamp == null) ? Instant.now() : timestamp;
        this.data = data;
        this.eventType = data.getClass();
        this.type = eventType.getCanonicalName();
        this.name = eventType.getSimpleName();
        this.partitionKey = partitionKey;
        this.messageKey = messageKey;
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

    public UUID getPartitionKey() {
        return partitionKey;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public T getData() {
        return data;
    }

    public UUID getId() {
        return id;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    @Override
    public String toString() {
        return "Event{" +
            "id=" + id +
            ", timestamp=" + timestamp +
            ", type='" + type + '\'' +
            ", name='" + name + '\'' +
            ", data=" + data +
            ", partitionKey=" + partitionKey +
            ", messageKey='" + messageKey + '\'' +
            ", eventType=" + eventType +
            '}';
    }

    public Event<T> withCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
        return this;
    }
}
