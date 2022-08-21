package io.richard.event.annotations;

public interface EventRecordHeaders {

    String CORRELATION_ID = "correlationId";
    String SOURCE_TOPIC = "sourceTopic";
    String ID = "id";
    String PARENT_ID = "parentId";
    String OBJECT_TYPE = "objectType";
    String PARTITION_KEY = "partitionKey";
    String SIMPLE_OBJECT_TYPE = "simpleObjectType";
    String SOURCE = "correlationId";
    String TIMESTAMP = "timestamp";
    String PUBLISHED_TIMESTAMP = "publishedTimestamp";
    String EVENT_TIMESTAMP = "eventTimestamp";
    String EVENT_VERSION = "eventVersion";
    String EVENT_PRIORITY = "eventPriority";
    String CLOUD_EVENT_TRACE_ID = "ce-trace-id";
    String IS_EVENT_DEAD = "is-dead";
    String CONTENT_TYPE = "contentType";
    String MAX_RETRY = "maxRetry";
    String RETRY_COUNT = "retryCount";
    String NEXT_RETRY = "nextRetry";
}