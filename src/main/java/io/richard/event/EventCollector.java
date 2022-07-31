package io.richard.event;

import jakarta.inject.Singleton;
import java.util.Collection;
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
