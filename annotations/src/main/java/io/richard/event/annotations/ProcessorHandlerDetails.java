package io.richard.event.annotations;

public record ProcessorHandlerDetails(
    int methodParamCount,
    Class<?> handlerProxy
) {
}
