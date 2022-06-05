package io.richard.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import java.io.IOException;

@Singleton
public class EventJsonParser {

    public static final String OBJECT_TYPE = "type";
    public static final String EVENT_DATA = "data";
    private final ObjectMapper objectMapper;
    private final ClassLoader classLoader;

    public EventJsonParser(ObjectMapper objectMapper) {
        classLoader = this.getClass().getClassLoader();
        this.objectMapper = objectMapper;
    }

    @SuppressWarnings("unchecked")
    <T> Event<T> parse(byte[] data) throws IOException, ClassNotFoundException {
        JsonNode jsonNode = objectMapper.readTree(data);
        if (jsonNode == null) {
            return null;
        }

        String objectType = jsonNode.get(OBJECT_TYPE).asText();
        if (objectType == null || objectType.isEmpty()) {
            return null;
        }

        Class<?> clzz = classLoader.loadClass(objectType);
        JsonNode dataJsonNode = jsonNode.get(EVENT_DATA);
        if (dataJsonNode == null) {
            return null;
        }

        T eventObject = (T) objectMapper.treeToValue(dataJsonNode, clzz);
        return new Event<>(eventObject);
    }
}
