package io.richard.common.jackson;

import static io.richard.common.jackson.EventRecordSerializer.EVENT_METADATA_KEY;
import static io.richard.common.jackson.EventRecordSerializer.EXCEPTION_SUMMARY_KEY;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.richard.event.annotations.EventMetadata;
import io.richard.event.annotations.EventRecord;
import io.richard.event.annotations.ExceptionSummary;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

public class EventRecordDeserializer extends JsonDeserializer<EventRecord> {

    @Override
    public EventRecord deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException {
        ObjectMapper objectMapper = (ObjectMapper) jp.getCodec();
        JsonNode node = objectMapper.readTree(jp);
        byte[] rawData = objectMapper.writeValueAsBytes(jp);
        System.out.println(objectMapper.writeValueAsString(jp));
        TreeNode metadata = node.get(EVENT_METADATA_KEY);
        if (metadata == null) {
            throw new IllegalStateException("metadata object required to be deserialized");
        }
        EventMetadata eventMetadata = objectMapper.readValue(metadata.traverse(), EventMetadata.class);

        String eventType = node.get("ce_type").asText();
        if (isNullOrEmpty(eventType)) {
            throw new IllegalStateException("event type object required to be deserialized");
        }

        String eventSource = node.get("ce_source").asText();
        String simpleType = node.get("ce_simple_type").asText();

        String eventId = node.get("ce_id").asText();
        if (isNullOrEmpty(eventType)) {
            throw new IllegalStateException("event id field required to be deserialized");
        }
        String timestamp = node.get("ce_timestamp").asText();
        Instant eventTimestamp;
        if (isNullOrEmpty(timestamp)) {
            eventTimestamp = Instant.now();
        } else {
            eventTimestamp = Instant.parse(timestamp);
        }

        Class<?> eventClass;
        try {
            eventClass = Class.forName(eventType);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        JsonNode dataJsonNode = node.get("data");
        if (dataJsonNode == null) {
            throw new IllegalStateException("no data node to process event");
        }

        Object eventObject = objectMapper.treeToValue(dataJsonNode, eventClass);

        TreeNode exceptionSummaryNode = node.get(EXCEPTION_SUMMARY_KEY);
        ExceptionSummary exceptionSummary = null;
        if (exceptionSummaryNode != null) {
            exceptionSummary = objectMapper.readValue(exceptionSummaryNode.traverse(), ExceptionSummary.class);
        }

        return new EventRecord(UUID.fromString(eventId), eventSource, eventType, eventTimestamp, simpleType,
            eventMetadata, eventClass, rawData, eventObject, new HashMap<>(), exceptionSummary);
    }

    boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }
}