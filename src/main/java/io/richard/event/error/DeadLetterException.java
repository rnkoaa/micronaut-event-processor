package io.richard.event.error;

public class DeadLetterException extends RuntimeException {
    public DeadLetterException(String message) {
        super(message);
    }
}
