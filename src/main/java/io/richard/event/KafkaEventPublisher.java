package io.richard.event;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.EventRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Collection;
import java.util.UUID;

@KafkaClient
public interface KafkaEventPublisher {

    @Topic("${product.stream.topic}")
    void publish(@KafkaKey UUID partitionKey, EventRecord eventRecord);

    @Topic("${product.stream.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey UUID partitionKey, EventRecord eventRecord);

    @Topic("${product.stream.topic}")
    void publish(@KafkaKey UUID partitionKey, Headers headers, EventRecord eventRecord);

    @Topic("${product.stream.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey UUID partitionKey, Headers headers, EventRecord eventRecord);

    @Topic("${product.stream.topic}")
    void publish(@KafkaKey UUID partitionKey, Collection<Header> headers, EventRecord eventRecord);

    @Topic("${product.stream.dead-letter.topic}")
    void publishDeadLetter(@KafkaKey UUID partitionKey, Collection<Header> headers, EventRecord eventRecord);
}
