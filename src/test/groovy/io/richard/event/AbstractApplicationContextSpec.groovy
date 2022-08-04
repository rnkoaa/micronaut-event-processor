package io.richard.event

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.ResourceLoader
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification


abstract class AbstractApplicationContextSpec extends Specification implements ContextFixture {

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

    @Override
    Map<String, Object> getConfiguration() {
        return [
                "app.event.retry.topic": "event-retry"
        ]
    }
}