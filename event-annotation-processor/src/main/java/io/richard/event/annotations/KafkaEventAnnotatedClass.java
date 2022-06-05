package io.richard.event.annotations;

import javax.lang.model.element.TypeElement;

public record KafkaEventAnnotatedClass(
    TypeElement element
) {

    public String id() {
        return element.getSimpleName().toString();
    }

}
