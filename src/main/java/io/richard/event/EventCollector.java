package io.richard.event;

import io.richard.event.annotations.EventRecord;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

@Singleton
public class EventCollector {
    private static final Collection<Object> records = new ConcurrentLinkedDeque<>();

    public int size() {
        return records.size();
    }

    public void add(Object record) {
        records.add(record);
    }

//    public void remove(UUID id) {
//        records.removeIf(rec -> rec.id().equals(id));
//    }

    public Collection<Object> all() {
        return records;
    }

    public void clear() {
        records.clear();
    }

    public Object next() {
        return records.iterator().next();
    }
}
