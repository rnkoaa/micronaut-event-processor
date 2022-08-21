package io.richard.event.annotations;

import static io.richard.event.annotations.EventRecordHeaders.CORRELATION_ID;
import static io.richard.event.annotations.EventRecordHeaders.ID;
import static io.richard.event.annotations.EventRecordHeaders.OBJECT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.PARTITION_KEY;
import static io.richard.event.annotations.EventRecordHeaders.SIMPLE_OBJECT_TYPE;
import static io.richard.event.annotations.EventRecordHeaders.SOURCE;
import static io.richard.event.annotations.EventRecordHeaders.TIMESTAMP;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public record EventRecord(
    UUID id,
    Object data,
    String source,
    UUID correlationId,
    String partitionKey,
    EventMetadata metadata,
    Map<String, Object> headers,
    ExceptionSummary exceptionSummary
) {

    public EventRecord {
        if (data == null) {
            throw new IllegalStateException("data cannot be null for an event record");
        }
        if (headers == null) {
            headers = new HashMap<>();
        } else {
            headers = new HashMap<>(headers);
        }

        if (correlationId != null) {
            headers.put(CORRELATION_ID, correlationId);
        }
        if (partitionKey != null) {
            headers.put(PARTITION_KEY, partitionKey);
        }

        String objectType = data.getClass().getCanonicalName();
        headers.computeIfAbsent(OBJECT_TYPE, k -> objectType);
        headers.computeIfAbsent(SIMPLE_OBJECT_TYPE, k -> objectType.replaceFirst(".*\\.", ""));
        headers.computeIfAbsent(TIMESTAMP, k -> Instant.now().toString());
        headers.computeIfAbsent(ID, k -> (id != null) ? id : UUID.randomUUID());
        headers.computeIfAbsent(SOURCE, k -> source != null ? source : "");
    }

    public EventRecord(Object data, UUID correlationId, String partitionKey) {
        this(UUID.randomUUID(), data, "", correlationId, partitionKey,
            new EventMetadata(data.getClass(), correlationId, partitionKey),
            Map.of(
                CORRELATION_ID, correlationId,
                PARTITION_KEY, partitionKey
            ), null);
    }
}

//
//public record EventRecord(
//    UUID id,
//    String source,
//    String type,
//    Instant timestamp,
//
//    @JsonProperty("simple_class_name")
//    String simpleClassName,
//    EventMetadata metadata,
//
//    @JsonIgnore
//    Class<?> eventClass,
//
//    @JsonIgnore
//    byte[] rawMessage,
//
//    @Nonnull
//    Object data,
//
//    Map<String, Object> headers,
//
//    @Nullable
//    @JsonProperty("exception_summary")
//    ExceptionSummary exceptionSummary
//) {
//
//    public EventRecord {
//        Objects.requireNonNull(data);
//        Objects.requireNonNull(headers);
//
//        if (timestamp == null) {
//            timestamp = Instant.now();
//        }
//        if (metadata == null) {
//            throw new IllegalArgumentException("timestamp cannot be null");
//        }
//        if (simpleClassName == null || simpleClassName.isEmpty()) {
//            simpleClassName = type.split("\\.")[type.length() - 1];
//        }
//    }
//
//    private EventRecord(Object data) {
//        this(UUID.randomUUID(), System.getenv("USER"), data.getClass().getCanonicalName(), Instant.now(),
//            data.getClass().getSimpleName(), new EventMetadata(), data.getClass(),
//            null, data, new HashMap<>(), null);
//    }
//
//    public EventRecord(UUID id, String source, Object data, EventMetadata metadata) {
//        this(id, source, data.getClass().getCanonicalName(), Instant.now(),
//            data.getClass().getSimpleName(), metadata, data.getClass(),
//            null, data, new HashMap<>(), null);
//    }
//
//    public EventRecord(UUID id, String source, Object data) {
//        this(id, source, data.getClass().getCanonicalName(), Instant.now(),
//            data.getClass().getSimpleName(), new EventMetadata(), data.getClass(),
//            null, data, new HashMap<>(), null);
//    }
//
//    public EventRecord withRawMessage(byte[] rawMessage) {
//        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
//            headers, exceptionSummary);
//    }
//
//    public EventRecord withData(Object data) {
//        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
//            headers, exceptionSummary);
//    }
//
//    public EventRecord withHeaders(Map<String, Object> headers) {
//        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
//            headers, exceptionSummary);
//    }
//
//    public <T> T getTypedData(Class<T> typedClass) {
//        return typedClass.cast(data);
//    }
//
//    public <T> Event<T> getEvent(Class<T> typedClass) {
//        //add other details here for the event
//        UUID partitionKey = (UUID) headers.getOrDefault("partitionKey", UUID.randomUUID());
//        String messageKey = (String) headers.getOrDefault("messageKey", "");
//        var event = new Event<>(id, timestamp, partitionKey, messageKey, typedClass.cast(data));
//        return event.withCorrelationId(metadata.correlationId());
//    }
//
//    public EventRecord copy() {
//        var headerCopy = new HashMap<>(headers);
//
//        return new EventRecord(
//            id,
//            source,
//            type,
//            timestamp,
//            simpleClassName,
//            metadata.copy(),
//            eventClass,
//            rawMessage,
//            data,
//            headerCopy,
//            exceptionSummary
//        );
//    }
//
//    public EventRecord withMetadata(EventMetadata eventMetadata) {
//        return new EventRecord(id, source, type, timestamp, simpleClassName, eventMetadata, eventClass, rawMessage, data,
//            headers, exceptionSummary);
//    }
//
//    public EventRecord withException(ExceptionSummary exception) {
//        return new EventRecord(id, source, type, timestamp, simpleClassName, metadata, eventClass, rawMessage, data,
//            headers, exception);
//    }
//}
