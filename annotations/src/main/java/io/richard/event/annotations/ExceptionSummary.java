package io.richard.event.annotations;

public record ExceptionSummary(String message) {
    public ExceptionSummary(Throwable ex) {
        this(ex.getMessage());
    }
}
