package io.richard.event;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.messaging.annotation.MessageBody;
import io.richard.event.annotations.ErrorContext;
import io.richard.event.annotations.EventRecord;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Collection;
import java.util.UUID;

@KafkaClient
public interface KafkaEventPublisher {

    @Topic("${app.event.topic}")
    void publish(@KafkaKey UUID partitionKey, EventRecord eventRecord);

    @Topic("${app.event.topic}")
    void publish(@KafkaKey UUID partitionKey, EventRecord eventRecord, Headers headers);

    @Topic("${app.event.topic}")
    void publish(@KafkaKey UUID partitionKey, EventRecord eventRecord, Collection<Header> headers);

    @Topic("${app.event.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey UUID partitionKey, EventRecord eventRecord);

    @Topic("${app.event.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey String partitionKey, @MessageBody ErrorContext errorContext, Headers headers);

    @Topic("${app.event.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey UUID partitionKey, EventRecord eventRecord, Headers headers);

    @Topic("${app.event.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey UUID partitionKey, EventRecord eventRecord, Collection<Header> headers);

    @Topic("${app.event.retry.topic}")
    void publishRetry(UUID partitionKey, EventRecord retryEventRecord);

    @Topic("${app.event.retry.topic}")
    void publishRetry(@KafkaKey UUID partitionKey, EventRecord eventRecord, Collection<Header> headers);

    @Topic("${app.event.retry.topic}")
    void publishRetry(@KafkaKey UUID partitionKey, EventRecord eventRecord, Headers headers);
}
