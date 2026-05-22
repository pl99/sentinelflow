package com.sentinelflow.flink.detector.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;

public final class FlinkSerialization {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private FlinkSerialization() {}

    public static <T> JacksonKafkaDeserializer<T> deserializer(Class<T> clazz) {
        return new JacksonKafkaDeserializer<>(clazz);
    }

    @SuppressWarnings("unchecked")
    public static <T> SerializationSchema<T> serializer() {
        return (SerializationSchema<T>) (SerializationSchema<Object>)
                element -> {
                    try {
                        return MAPPER.writeValueAsBytes(element);
                    } catch (IOException e) {
                        throw new RuntimeException("Serialization failed for " + element, e);
                    }
                };
    }
}
