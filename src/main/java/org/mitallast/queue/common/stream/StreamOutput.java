package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.settings.Settings;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface StreamOutput extends DataOutput, Closeable {

    @Override
    void write(int b) throws IOException;

    @Override
    void write(byte[] b) throws IOException;

    @Override
    void write(byte[] b, int off, int len) throws IOException;

    @Override
    void writeBoolean(boolean v) throws IOException;

    @Override
    void writeByte(int v) throws IOException;

    @Override
    void writeShort(int v) throws IOException;

    @Override
    void writeChar(int v) throws IOException;

    @Override
    void writeInt(int v) throws IOException;

    @Override
    void writeLong(long v) throws IOException;

    @Override
    void writeFloat(float v) throws IOException;

    @Override
    void writeDouble(double v) throws IOException;

    default void writeText(String text) throws IOException {
        writeUTF(text);
    }

    default void writeTextOrNull(String text) throws IOException {
        if (text == null || text.isEmpty()) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeUTF(text);
        }
    }

    default void writeUUID(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    default void writeUUIDOrNull(UUID uuid) throws IOException {
        if (uuid != null) {
            writeLong(uuid.getMostSignificantBits());
            writeLong(uuid.getLeastSignificantBits());
        } else {
            writeLong(0);
            writeLong(0);
        }
    }

    default <Type extends Enum<Type>> void writeEnum(Type type) throws IOException {
        writeInt(type.ordinal());
    }

    default <Type extends Enum<Type>> void writeEnumOrNull(Type type) throws IOException {
        if (type != null) {
            writeInt(type.ordinal());
        } else {
            writeInt(-1);
        }
    }

    void writeByteBuf(ByteBuf buffer) throws IOException;

    void writeByteBuf(ByteBuf buffer, int length) throws IOException;

    void writeByteBufOrNull(ByteBuf buffer) throws IOException;

    void writeByteBufOrNull(ByteBuf buffer, int length) throws IOException;

    <T extends Streamable> void writeClass(Class<T> streamableClass) throws IOException;

    default void writeSettings(Settings settings) throws IOException {
        Map<String, String> asMap = settings.getAsMap();
        writeInt(asMap.size());
        for (Map.Entry<String, String> entry : asMap.entrySet()) {
            writeUTF(entry.getKey());
            writeUTF(entry.getValue());
        }
    }

    default <T extends Streamable> void writeStreamable(T streamable) throws IOException {
        streamable.writeTo(this);
    }

    default <T extends Streamable> void writeStreamableOrNull(T streamable) throws IOException {
        if (streamable != null) {
            writeBoolean(true);
            streamable.writeTo(this);
        } else {
            writeBoolean(false);
        }
    }

    default <T extends Streamable> void writeStreamableList(List<T> streamable) throws IOException {
        int size = streamable.size();
        writeInt(size);
        if (size > 0) {
            for (T t : streamable) {
                t.writeTo(this);
            }
        }
    }

    default <T extends Streamable> void writeStreamableSet(Set<T> streamable) throws IOException {
        int size = streamable.size();
        writeInt(size);
        if (size > 0) {
            for (T t : streamable) {
                t.writeTo(this);
            }
        }
    }

    default <T extends StreamableError> void writeError(T error) throws IOException {
        StreamableError.Builder builder = error.toBuilder();
        writeClass(builder.getClass());
        writeStreamable(builder);
    }
}
