package io.richard.event.annotations;

import java.util.Optional;

public interface DependencyInjectionProvider {

    <T> Optional<T> getBean(Class<?> clazz);
}
