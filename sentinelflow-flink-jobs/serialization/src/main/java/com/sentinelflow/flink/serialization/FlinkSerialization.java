package com.sentinelflow.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.IOException;

public final class FlinkSerialization {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private FlinkSerialization() {}

    public static <T> JacksonKafkaDeserializer<T> deserializer(Class<T> clazz) {
        return new JacksonKafkaDeserializer<>(clazz);
    }

    public static <T> SerializationSchema<T> serializer(Class<T> typeClass) {
        return new JacksonSerializationSchema<>(typeClass);
    }

    private static class JacksonSerializationSchema<T> implements SerializationSchema<T>, ResultTypeQueryable<T> {
        private final Class<T> typeClass;

        JacksonSerializationSchema(Class<T> typeClass) {
            this.typeClass = typeClass;
        }

        @Override
        public byte[] serialize(T element) {
            try {
                return MAPPER.writeValueAsBytes(element);
            } catch (IOException e) {
                throw new RuntimeException("Serialization failed for " + element, e);
            }
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return TypeInformation.of(typeClass);
        }
    }
}
