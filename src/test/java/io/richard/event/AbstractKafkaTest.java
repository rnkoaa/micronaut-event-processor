package io.richard.event;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@Testcontainers
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractKafkaTest implements TestPropertyProvider {
    static final String KAFKA_DOCKER_IMAGE = "confluentinc/cp-kafka:7.2.0";

    @Container
    static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));

    @Override
    public @NotNull Map<String, String> getProperties() {
        var props = new HashMap<>(additionalProperties());
        props.putAll(
            Map.of(
                "app.event.groupId", "product-stream-group-01",
                "micronaut.application.name", "producer-test-application",
                "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers()
            )
        );
        return Map.copyOf(props);
    }

    protected abstract Map<String, String> additionalProperties();
}
