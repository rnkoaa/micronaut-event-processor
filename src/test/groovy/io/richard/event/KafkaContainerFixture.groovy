package io.richard.event

import org.testcontainers.containers.KafkaContainer

trait KafkaContainerFixture extends ContextFixture {

    @Override
    Map<String, Object> getConfiguration() {
        KafkaContainer kafkaContainer = KafkaTestContainer.kafkaContainer
        if (kafkaContainer == null || !kafkaContainer.isRunning()) {
            KafkaTestContainer.init()
            kafkaContainer = KafkaTestContainer.kafkaContainer
        }

        Map<String, Object> configs = [:]

        if (specName) {
            configs["spec.name"] = specName
        }

        configs += [
                "app.event.retry.topic"     : "product-stream-retry",
                "app.event.groupId"         : "product-stream-group-01",
                "micronaut.application.name": "producer-test-application",
                "kafka.bootstrap.servers"   : kafkaContainer.getBootstrapServers()
        ]

        if (additionalProperties != null) {
            configs += additionalProperties
        }

        return configs
    }


}