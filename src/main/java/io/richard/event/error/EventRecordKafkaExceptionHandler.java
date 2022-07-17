package io.richard.event.error;

import io.micronaut.configuration.kafka.exceptions.DefaultKafkaListenerExceptionHandler;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler;
import io.micronaut.context.annotation.Replaces;
import io.richard.event.annotations.ErrorContext;
import io.richard.event.annotations.ExceptionSummary;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Singleton
@Replaces(DefaultKafkaListenerExceptionHandler.class)
public class EventRecordKafkaExceptionHandler extends DefaultKafkaListenerExceptionHandler implements KafkaListenerExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventRecordKafkaExceptionHandler.class);
    private final ErrorEventPublisher errorEventPublisher;

    public EventRecordKafkaExceptionHandler(ErrorEventPublisher errorEventPublisher) {
        this.errorEventPublisher = errorEventPublisher;
    }

    @Override
    public void handle(KafkaListenerException exception) {
        LOGGER.error("Got an exception {}", exception.getMessage());
        var maybeConsumerRecord = exception.getConsumerRecord();
        String message = exception.getMessage();


        var deadRecord = maybeConsumerRecord
            .map(consumerRecord -> convertRecord(consumerRecord, exception.getMessage()))

            // consumerRecord does not exist, so no way to get original headers
            .orElse(new DeadLetterEventRecord(new ErrorContext(message), Map.of()));

        try {
            errorEventPublisher.publishError(deadRecord.id(), deadRecord);
        } catch (Exception ex) {
            LOGGER.info("error while publishing bad message {}", ex.getMessage());
        }

        super.handle(exception);
        // we need to commit this message
        exception.getKafkaConsumer().commitSync();
    }

    DeadLetterEventRecord convertRecord(ConsumerRecord<?, ?> consumerRecord, String message) {
        var errorContext = new ErrorContext(
            consumerRecord.topic(), consumerRecord.value(),
            Instant.now(), consumerRecord.offset(),
            consumerRecord.key(), consumerRecord.partition(),
            new ExceptionSummary(message)
        );

        return new DeadLetterEventRecord(errorContext, mapHeaders(consumerRecord.headers()));
    }

    private static Map<String, Object> mapHeaders(Headers headers) {
        Map<String, Object> messageHeaders = new HashMap<>();
        if (headers != null) {
            while (headers.iterator().hasNext()) {
                Header next = headers.iterator().next();
                messageHeaders.put(next.key(), next.value());
            }
        }
        return messageHeaders;
    }
}
