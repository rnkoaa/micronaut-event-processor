package io.richard.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@KafkaListener(groupId = "${product.stream.groupId}")
public class ProductEventKafkaListener {

    private final EventProcessorGroup eventProcessorGroup;
    private final ObjectMapper objectMapper;

    public ProductEventKafkaListener(
        EventProcessorGroup eventProcessorGroup, ObjectMapper objectMapper) {
        this.eventProcessorGroup = eventProcessorGroup;
        this.objectMapper = objectMapper;
    }

    @Topic("${product.stream.topic}")
    void consumeEvents(ConsumerRecord<String, byte[]> consumerRecord,
                       Consumer<String, byte[]> kafkaConsumer) throws IOException, ClassNotFoundException {
        byte[] value = consumerRecord.value();

        Map<String, Object> headers = new HashMap<>();
        EventRecord eventRecord = objectMapper.readValue(value, EventRecord.class);
        for (Header next : consumerRecord.headers()) {
            if (next.key().equals("ce-trace-id")) {
                headers.put("ce-trace-id", new String(next.value()));
            }
            if (next.key().equals("correlation-id")) {
                headers.put("correlation-id", new String(next.value()));
            }
        }

        if (!headers.containsKey("ce-trace-id")) {
            headers.put("ce-trace-id", UUID.randomUUID().toString());
        }

        if (!headers.containsKey("correlation-id")) {
            headers.put("correlation-id", UUID.randomUUID().toString());
        }

        var finalEventRecord = eventRecord.withHeaders(headers);
        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
        eventProcessorGroup.processEvent(event);

        kafkaConsumer.commitSync(Collections.singletonMap(
            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
            new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")
        ));
    }
}
