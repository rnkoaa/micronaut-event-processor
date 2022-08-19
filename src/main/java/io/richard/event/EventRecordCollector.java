package io.richard.event;

import io.richard.event.annotations.EventRecord;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

@Singleton
public class EventRecordCollector {
    private static final Collection<EventRecord> records = new ConcurrentLinkedDeque<>();

    public int size() {
        return records.size();
    }

    public void add(EventRecord record) {
        records.add(record);
    }

    public void remove(UUID id) {
//        records.removeIf(rec -> rec.id().equals(id));
    }

    public Collection<EventRecord> all() {
        return records;
    }

    public void clear() {
        records.clear();
    }

    public EventRecord next() {
        return records.iterator().next();
    }
}
