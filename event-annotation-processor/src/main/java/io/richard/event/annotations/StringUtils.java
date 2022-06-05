package io.richard.event.annotations;

public class StringUtils {
    public static String toCamelCase(String value) {
        char firstChar = Character.toLowerCase(value.charAt(0));
        return firstChar + value.substring(1);
    }

    public static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

    public static boolean isNullOrNullString(String value) {
        return value == null || "null".equals(value);
    }
}