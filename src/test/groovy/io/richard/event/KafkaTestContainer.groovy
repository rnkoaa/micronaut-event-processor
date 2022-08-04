package io.richard.event

import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

class KafkaTestContainer {

    static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";

    static KafkaContainer kafkaContainer

    static void init() {
        if (kafkaContainer == null) {
            kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));
            kafkaContainer.start()
        }
    }
}
