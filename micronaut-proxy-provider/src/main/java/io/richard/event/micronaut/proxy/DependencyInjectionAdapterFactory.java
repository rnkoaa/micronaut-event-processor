package io.richard.event.micronaut.proxy;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.richard.event.annotations.DependencyInjectionProvider;
import jakarta.inject.Singleton;
import java.util.Optional;

@Factory
public class DependencyInjectionAdapterFactory {

    @Singleton
    public DependencyInjectionProvider provides(ApplicationContext applicationContext) {
        return new DependencyInjectionProvider() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> Optional<T> getBean(Class<?> clazz) {
                Object bean = applicationContext.getBean(clazz);
                return (Optional<T>) Optional.of(bean);
            }
        };
    }
}
