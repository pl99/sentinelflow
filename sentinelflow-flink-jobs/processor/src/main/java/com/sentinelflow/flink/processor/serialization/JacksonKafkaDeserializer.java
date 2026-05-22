package com.sentinelflow.flink.processor.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class JacksonKafkaDeserializer<T> implements KafkaRecordDeserializationSchema<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Class<T> typeClass;
    private transient JacksonTypeInfo<T> typeInfo;

    public JacksonKafkaDeserializer(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out) throws IOException {
        if (record.value() == null || record.value().length == 0) return;
        T value = MAPPER.readValue(record.value(), typeClass);
        out.collect(value);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        if (typeInfo == null) {
            typeInfo = new JacksonTypeInfo<>(typeClass);
        }
        return typeInfo;
    }
}
