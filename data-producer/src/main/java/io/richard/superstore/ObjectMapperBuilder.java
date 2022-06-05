package io.richard.superstore;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ObjectMapperBuilder {

    public static CsvMapper buildCsvObjectMapper() {
        var csvMapper = new CsvMapper();
        csvMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        csvMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        // Ignore null values when writing json.
        csvMapper.setSerializationInclusion(Include.NON_DEFAULT);

        // Write times as a String instead of a Long so its human readable.
        csvMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        csvMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        csvMapper.registerModule(new JavaTimeModule());
        csvMapper.registerModule(new Jdk8Module());
        return csvMapper;
    }
}
