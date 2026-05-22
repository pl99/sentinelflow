package com.sentinelflow.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class KafkaConfig {

    public static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static <T> Serializer<T> jsonSerializer() {
        return new JsonSerializer<>(objectMapper());
    }

    public static <T> Deserializer<T> jsonDeserializer(Class<T> targetType) {
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetType, objectMapper());
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("com.sentinelflow.common");
        return deserializer;
    }

    private KafkaConfig() {}
}
