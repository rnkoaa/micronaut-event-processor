package io.richard.common.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.richard.event.annotations.EventRecord;

import java.io.IOException;
import java.util.UUID;

public class EventRecordSerializer extends JsonSerializer<EventRecord> {

    public static final String EVENT_METADATA_KEY = "metadata";
    public static final String EXCEPTION_SUMMARY_KEY = "exception_summary";
    public static final String EVENT_DATA_KEY = "data";

    @Override
    public void serialize(EventRecord value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField(EVENT_METADATA_KEY, value.metadata());
        gen.writeStringField("ce_source", value.source());
        gen.writeStringField("ce_type", value.type());
        gen.writeStringField("ce_simple_type", value.type());
        gen.writeStringField("ce_id", (value.id() != null) ? value.id().toString() : UUID.randomUUID().toString());
        gen.writeStringField("ce_timestamp", value.timestamp().toString());
        if (value.data() != null) {
            gen.writeObjectField(EVENT_DATA_KEY, value.data());
        }
        if (value.exceptionSummary() != null) {
            gen.writeObjectField(EXCEPTION_SUMMARY_KEY, value.exceptionSummary());
        }
        gen.writeEndObject();
    }
}