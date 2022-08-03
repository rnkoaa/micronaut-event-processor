package io.richard.event.error;

public class RetryableException extends RuntimeException {

    public RetryableException(String message) {
        super(message);
    }
}
