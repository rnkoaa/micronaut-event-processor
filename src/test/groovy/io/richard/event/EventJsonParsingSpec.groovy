package io.richard.event

import io.richard.event.annotations.ErrorContext
import io.richard.event.annotations.Event
import io.richard.event.annotations.EventMetadata
import io.richard.event.annotations.EventRecord
import io.richard.event.annotations.ExceptionSummary

class EventJsonParsingSpec extends AbstractApplicationContextSpec {

    void "parse product created event"() {
        given:
        String path = "data/product_created_event.json"
        Optional<InputStream> resourceAsStream = resourceLoader.getResourceAsStream(path)

        expect:
        resourceAsStream.present

        when:
        byte[] bytes = resourceAsStream.get().readAllBytes()
        EventRecord eventRecord = objectMapper.readValue(bytes, EventRecord)

        then:
        Event event = eventRecord.getEvent(eventRecord.eventClass())
        event.eventType == ProductCreatedEvent

        and:
        ProductCreatedEvent data = (ProductCreatedEvent) event.getData()
        data == new ProductCreatedEvent(UUID.fromString("9bb3d5a7-30a3-4875-a9d0-13be882bb35d"), "product 1")
    }

    void "dead letter event serde test"() {
        given:
        var deadRecord = new DeadLetterEventRecord(new ErrorContext("Bad Message"), [:])
        String deadRecordStr = objectMapper.writeValueAsString(deadRecord)

        and:
        DeadLetterEventRecord deadLetterEventRecord = objectMapper.readValue(deadRecordStr, DeadLetterEventRecord.class)

        expect:
        deadLetterEventRecord != null
    }

    void "event record with exception can be parsed"() {
        given:
        var productCreatedEvent = new ProductCreatedEvent(UUID.randomUUID(), "Trek Bike Series 7")
        var deadEvent = new EventMetadata().withDead(true)

        and:
        EventRecord eventRecord = new EventRecord(UUID.randomUUID(),
                "test", productCreatedEvent, deadEvent)
                .withException(new ExceptionSummary("Bad serialization"))

        String messageStr = objectMapper.writeValueAsString(eventRecord)

        expect:
        messageStr

        when:
        EventRecord overAndBack = objectMapper.readValue(messageStr, EventRecord)

        then:
        overAndBack != null
        overAndBack.exceptionSummary() == eventRecord.exceptionSummary()
        overAndBack.data() == eventRecord.data()
        overAndBack.metadata() == eventRecord.metadata()
    }

}