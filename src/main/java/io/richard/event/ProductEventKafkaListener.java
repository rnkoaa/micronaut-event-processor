package io.richard.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.EventProcessorGroup;
import io.richard.event.annotations.EventRecord;
import java.io.IOException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@KafkaListener(groupId = "product-event-consumer-01")
public class ProductEventKafkaListener {

    private final EventProcessorGroup eventProcessorGroup;
    private final ObjectMapper objectMapper;

    public ProductEventKafkaListener(
        EventProcessorGroup eventProcessorGroup,  ObjectMapper objectMapper) {
        this.eventProcessorGroup = eventProcessorGroup;
        this.objectMapper = objectMapper;
    }

    @Topic("product-stream")
    @SuppressWarnings("unchecked")
    void consumeEvents(ConsumerRecord<String, byte[]> consumerRecord,
        Consumer<String, byte[]> kafkaConsumer) throws IOException, ClassNotFoundException {
        byte[] value = consumerRecord.value();

        EventRecord eventRecord = objectMapper.readValue(value, EventRecord.class);

//        eventProcessorGroup.processEvent(event);
//        System.out.println("Done processing event from kafka");
//
//        kafkaConsumer.commitSync(Collections.singletonMap(
//            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
//            new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")
//        ));
    }
}
