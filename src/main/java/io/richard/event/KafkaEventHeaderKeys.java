package io.richard.event;

public interface KafkaEventHeaderKeys {
    String KAFKA_CORRELATION_ID = "correlation-id";
    String KAFKA_SOURCE_TOPIC = "source-topic";
    String KAFKA_ID = "id";
    String KAFKA_PARENT_ID = "parent-id";
    String KAFKA_OBJECT_TYPE = "object-type";
    String KAFKA_PARTITION_KEY = "partition-key";
    String KAFKA_SIMPLE_OBJECT_TYPE = "simple-object-type";
    String KAFKA_SOURCE = "source";
    String KAFKA_TIMESTAMP = "timestamp";
    String KAFKA_PUBLISHED_TIMESTAMP = "published-timestamp";
    String KAFKA_EVENT_TIMESTAMP = "event-timestamp";
    String KAFKA_EVENT_VERSION = "event-version";
    String KAFKA_EVENT_PRIORITY = "event-priority";
    String KAFKA_CLOUD_EVENT_TRACE_ID = "ce-trace-id";
    String KAFKA_IS_EVENT_DEAD = "is-dead";
    String KAFKA_CONTENT_TYPE = "content-type";
    String KAFKA_MAX_RETRY = "max-retry";
    String KAFKA_RETRY_COUNT = "retry-count";
    String KAFKA_NEXT_RETRY = "next-retry";
}
