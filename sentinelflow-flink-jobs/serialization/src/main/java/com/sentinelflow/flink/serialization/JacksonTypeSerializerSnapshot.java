package com.sentinelflow.flink.serialization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class JacksonTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    private Class<T> type;

    public JacksonTypeSerializerSnapshot() {}

    public JacksonTypeSerializerSnapshot(Class<T> type) {
        this.type = type;
    }

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(type.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        String className = in.readUTF();
        try {
            this.type = (Class<T>) Class.forName(className, true, userCodeClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot find class " + className, e);
        }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new JacksonTypeSerializer<>(type);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializerSnapshot<T> newSnapshot) {
        if (newSnapshot instanceof JacksonTypeSerializerSnapshot) {
            JacksonTypeSerializerSnapshot<T> other = (JacksonTypeSerializerSnapshot<T>) newSnapshot;
            if (type != null && type.equals(other.type)) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
            return TypeSerializerSchemaCompatibility.incompatible();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public int hashCode() {
        return type != null ? type.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        JacksonTypeSerializerSnapshot<?> that = (JacksonTypeSerializerSnapshot<?>) obj;
        return type != null ? type.equals(that.type) : that.type == null;
    }
}
