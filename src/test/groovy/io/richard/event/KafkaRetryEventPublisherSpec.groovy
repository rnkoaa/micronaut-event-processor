package io.richard.event

import spock.lang.Specification


class KafkaRetryEventPublisherSpec extends Specification {
/*
private static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";
    private static final String ORDER_STREAM_TOPIC = "order-stream";
    private static final String ORDER_STREAM_DEAD_LETTER_TOPIC = "order-stream-dead-letter";

//    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));

    @Inject
    private KafkaEventPublisher kafkaEventPublisher;

    private static final Collection<EventRecord> eventRecordReceiver = new ConcurrentLinkedDeque<>();
    private static final Collection<EventRecord> deadLetterEventRecordReceiver = new ConcurrentLinkedDeque<>();

    @Inject
    ProductCreatedEventProcessor productCreatedEventProcessor;

//    @Test
    void assertKafkaIsRunning() {
        assertThat(kafkaContainer.isRunning()).isTrue();
    }

//    @Test
    void recordsPublishedWillBeSeenOnSameTopic() {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);
        kafkaEventPublisher.publish(productId, eventRecord);

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> eventRecordReceiver.size() > 0);

        EventRecord receivedRecord = eventRecordReceiver.iterator().next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

//    @Test
    void deadLetterEventsWillBeReadOnDeadLetterTopic() {
        var correlationId = UUID.randomUUID();
        var productId = UUID.randomUUID();
        String productName = "Diaper - Size 4";
        var eventMetadata = new EventMetadata(correlationId, ORDER_STREAM_TOPIC);
        var productCreatedEvent = new ProductCreatedEvent(productId, productName);
        var eventRecord = new EventRecord(UUID.randomUUID(), "test-source", productCreatedEvent, eventMetadata);

        kafkaEventPublisher.publish( productId, eventRecord);

        Awaitility.await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> deadLetterEventRecordReceiver.size() > 0);

        EventRecord receivedRecord = deadLetterEventRecordReceiver.iterator().next();

        Event<?> event = receivedRecord.getEvent(receivedRecord.eventClass());
        Object data = event.getData();
        assertThat(data.getClass()).isEqualTo(ProductCreatedEvent.class);
    }

//    @AfterEach
    void afterEach() {
        eventRecordReceiver.clear();
    }

    @Override
    public @NotNull Map<String, String> getProperties() {
        return Map.of(
            "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
            "app.event.topic", "product-stream-test"
        );
    }

//    @MockBean(ProductCreatedEventProcessor.class)
    ProductCreatedEventProcessor productCreatedEventProcessor(){
//        return Mockito.mock(ProductCreatedEventProcessor.class);
        return null;
    }

//    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
//    @Primary
    static class EventRecordListener {

        @Topic(ORDER_STREAM_TOPIC)
        void receive(EventRecord eventRecord) {
            eventRecordReceiver.add(eventRecord);
        }

        @Topic(ORDER_STREAM_DEAD_LETTER_TOPIC)
        void receiveDeadLetter(EventRecord eventRecord) {
            deadLetterEventRecordReceiver.add(eventRecord);
        }
    }
 */
}