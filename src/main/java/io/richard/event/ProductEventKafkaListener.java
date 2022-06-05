package io.richard.event;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.richard.event.annotations.Event;
import io.richard.event.annotations.EventProcessorGroup;
import java.io.IOException;
import java.util.Collections;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@KafkaListener(groupId = "product-event-consumer-01")
public class ProductEventKafkaListener {

    private final EventProcessorGroup eventProcessorGroup;
    private final EventJsonParser eventJsonParser;

    public ProductEventKafkaListener(
        EventProcessorGroup eventProcessorGroup, EventJsonParser eventJsonParser) {
        this.eventProcessorGroup = eventProcessorGroup;
        this.eventJsonParser = eventJsonParser;
    }

    @Topic("product-stream")
    @SuppressWarnings("unchecked")
    void consumeEvents(ConsumerRecord<String, byte[]> consumerRecord,
        Consumer kafkaConsumer) throws IOException, ClassNotFoundException {
        byte[] value = consumerRecord.value();

        Event<Object> event = eventJsonParser.parse(value);
        eventProcessorGroup.processEvent(event);
        System.out.println("Done processing event from kafka");

        kafkaConsumer.commitSync(Collections.singletonMap(
            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
            new OffsetAndMetadata(consumerRecord.offset() + 1, "my metadata")
        ));
    }
}
