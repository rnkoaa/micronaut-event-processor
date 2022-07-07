package io.richard.event;

public class Strings {
    public static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

    public static boolean isNotNullAndEmpty(String value) {
        return !isNullOrEmpty(value);
    }
}
