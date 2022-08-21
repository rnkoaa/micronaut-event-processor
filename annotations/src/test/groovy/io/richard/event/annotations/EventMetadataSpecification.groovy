package io.richard.event.annotations

import spock.lang.Specification

import java.time.Instant


class EventMetadataSpecification extends Specification {


    void "expect event Metadata"() {
        given:
        var item = new TestProductCreated(UUID.randomUUID(), Instant.now(), Instant.now())
        var eventMetadata = new EventMetadata(TestProductCreated, UUID.randomUUID(), item.id().toString());
        expect:
        eventMetadata != null
        eventMetadata.correlationId() != null
        eventMetadata.retry() != null
        !eventMetadata.retry().dead()
        eventMetadata.partitionKey() == item.id().toString()

        println eventMetadata
    }

}