package io.richard.event.error;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

import java.util.UUID;

@KafkaClient
public interface ErrorEventPublisher {

    /**
     * Error Topic is used whenever the consumer gets a message it cannot handle such as serialization
     * or deserialization error
     *
     * @param deadRecord the record being published. This wraps the original message, and its metadata
     */
    @Topic("${app.error.topic}")
    void publishError(@KafkaKey UUID partitionKey, DeadLetterEventRecord deadRecord);
}
