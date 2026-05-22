package com.sentinelflow.flink.detector.serialization;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class JacksonTypeInfo<T> extends TypeInformation<T> {

    private final Class<T> typeClass;

    public JacksonTypeInfo(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    @Override
    public boolean isBasicType() { return false; }

    @Override
    public boolean isTupleType() { return false; }

    @Override
    public int getArity() { return 1; }

    @Override
    public int getTotalFields() { return 1; }

    @Override
    public Class<T> getTypeClass() { return typeClass; }

    @Override
    public boolean isKeyType() { return false; }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new JacksonTypeSerializer<>(typeClass);
    }

    @Override
    public String toString() { return "JacksonTypeInfo<" + typeClass.getSimpleName() + ">"; }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || !(obj instanceof TypeInformation)) return false;
        return ((TypeInformation<?>) obj).getTypeClass() == typeClass;
    }

    @Override
    public int hashCode() { return typeClass.hashCode(); }

    @Override
    public boolean canEqual(Object obj) { return obj instanceof TypeInformation; }
}
