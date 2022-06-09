package io.richard.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

@KafkaListener(groupId = "product-event-consumer-01")
public class ProductEventKafkaListener {

    private final EventProcessorGroup eventProcessorGroup;
    private final ObjectMapper objectMapper;

    public ProductEventKafkaListener(
        EventProcessorGroup eventProcessorGroup, ObjectMapper objectMapper) {
        this.eventProcessorGroup = eventProcessorGroup;
        this.objectMapper = objectMapper;
    }

    @Topic("product-stream")
    @SuppressWarnings("unchecked")
    void consumeEvents(ConsumerRecord<String, byte[]> consumerRecord,
        Consumer<String, byte[]> kafkaConsumer) throws IOException, ClassNotFoundException {
        byte[] value = consumerRecord.value();

        Map<String, Object> headers = new HashMap<>();
        EventRecord eventRecord = objectMapper.readValue(value, EventRecord.class);
        for (Header next : consumerRecord.headers()) {
            if (next.key().equals("traceId")) {
                headers.put("traceId", new String(next.value()));
            }
            if (next.key().equals("correlationId")) {
                headers.put("correlationId", new String(next.value()));
            }
        }

        if(!headers.containsKey("traceId")) {
            headers.put("traceId", UUID.randomUUID().toString());
        }

        if(!headers.containsKey("correlationId")) {
            headers.put("correlationId", UUID.randomUUID().toString());
        }

        var finalEventRecord = eventRecord.withHeaders(headers);
        Event<?> event = finalEventRecord.getEvent(eventRecord.eventClass());
        eventProcessorGroup.processEvent(event);
//
//        kafkaConsumer.commitSync(Collections.singletonMap(
//            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
//            new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")
//        ));
    }
}
