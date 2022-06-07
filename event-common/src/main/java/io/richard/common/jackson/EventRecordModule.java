package io.richard.common.jackson;

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.richard.event.annotations.EventRecord;

public class EventRecordModule extends SimpleModule {

    private static final String NAME = "EventRecordModule";

    public EventRecordModule() {
        super(NAME);
        addSerializer(EventRecord.class, new EventRecordSerializer());
        addDeserializer(EventRecord.class, new EventRecordDeserializer());
    }
}
