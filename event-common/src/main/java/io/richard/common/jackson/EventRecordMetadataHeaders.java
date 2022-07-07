package io.richard.common.jackson;

/**
 * These values will appear inside the json body so its better to use underscores
 * than dashes which is different from {@code @EventRecordHeaders}
 */
public interface EventRecordMetadataHeaders {
    String SOURCE = "ce_source";
    String TYPE = "ce_type";
    String SIMPLE_TYPE = "ce_simple_type";
    String ID = "ce_id";
}
