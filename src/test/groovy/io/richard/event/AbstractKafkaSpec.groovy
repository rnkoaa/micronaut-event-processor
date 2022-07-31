package io.richard.event

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.ResourceLoader
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractKafkaSpec extends Specification implements KafkaContainerFixture {
    @AutoCleanup
    @Shared
    ApplicationContext context = ApplicationContext.run(configuration)

    @Shared
    ObjectMapper objectMapper

    @Shared
    @AutoCleanup
    ResourceLoader resourceLoader

    void setupSpec() {
        objectMapper = context.getBean(ObjectMapper)
        resourceLoader = context.getBean(ResourceLoader)
    }
}