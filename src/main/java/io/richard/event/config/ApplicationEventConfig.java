package io.richard.event.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("application.event")
public class ApplicationEventConfig {

    private boolean shouldDeadLetterUnhandled;
    private String deadLetterTopic;

    public void setShouldDeadLetterUnhandled(boolean shouldDeadLetterUnhandled) {
        this.shouldDeadLetterUnhandled = shouldDeadLetterUnhandled;
    }

    public boolean shouldDeadLetterUnhandled() {
        return shouldDeadLetterUnhandled;
    }

    public String getDeadLetterTopic() {
        return deadLetterTopic;
    }

    public void setDeadLetterTopic(String deadLetterTopic) {
        this.deadLetterTopic = deadLetterTopic;
    }
}
