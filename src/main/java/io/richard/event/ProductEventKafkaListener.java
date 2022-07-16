package io.richard.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.KafkaPartitionKey;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@KafkaListener(groupId = "${product.stream.groupId}", offsetReset = OffsetReset.EARLIEST)
public class ProductEventKafkaListener {
    private final EventRecordCollector eventRecordCollector;
    private static final Logger LOGGER = LoggerFactory.getLogger(ProductEventKafkaListener.class);

//    private final EventProcessorGroup eventProcessorGroup;
//    private final ObjectMapper objectMapper;

    public ProductEventKafkaListener(
        /*EventProcessorGroup eventProcessorGroup, ObjectMapper objectMapper*/
        EventRecordCollector eventRecordCollector
    ) {
//        this.eventProcessorGroup = eventProcessorGroup;
//        this.objectMapper = objectMapper;
        this.eventRecordCollector = eventRecordCollector;
    }

    @Topic("${product.stream.topic}")
    void consumeEvents(
        ConsumerRecord<String, EventRecord> consumerRecord,
        Consumer<String, byte[]> kafkaConsumer) throws IOException, ClassNotFoundException {
        EventRecord value = consumerRecord.value();
        String messageKey = consumerRecord.key();
        LOGGER.info("Got event with message key {}", messageKey);
        eventRecordCollector.add(value);
//
//        Map<String, Object> headers = new HashMap<>();
//        EventRecord eventRecord = objectMapper.readValue(value, EventRecord.class);
//        for (Header next : consumerRecord.headers()) {
//            if (next.key().equals("ce-trace-id")) {
//                headers.put("ce-trace-id", new String(next.value()));
//            }
//            if (next.key().equals("correlation-id")) {
//                headers.put("correlation-id", new String(next.value()));
//            }
//        }
//
//        if (!headers.containsKey("ce-trace-id")) {
//            headers.put("ce-trace-id", UUID.randomUUID().toString());
//        }
//
//        if (!headers.containsKey("correlation-id")) {
//            headers.put("correlation-id", UUID.randomUUID().toString());
//        }
//
//        var finalEventRecord = eventRecord.withHeaders(headers);
//        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
//        eventProcessorGroup.processEvent(event);

        kafkaConsumer.commitSync(Collections.singletonMap(
            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
            new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")
        ));
    }
}
