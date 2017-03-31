package org.mitallast.queue.common.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

public interface StreamInput extends Closeable {

    int available() throws IOException;

    void readFully(byte[] b) throws IOException;

    void readFully(byte[] b, int off, int len) throws IOException;

    int skipBytes(int n) throws IOException;

    boolean readBoolean() throws IOException;

    byte readByte() throws IOException;

    int readUnsignedByte() throws IOException;

    short readShort() throws IOException;

    int readUnsignedShort() throws IOException;

    char readChar() throws IOException;

    int readInt() throws IOException;

    long readLong() throws IOException;

    float readFloat() throws IOException;

    double readDouble() throws IOException;

    String readText() throws IOException;

    default <Type extends Enum<Type>> Type readEnum(Class<Type> enumClass) throws IOException {
        int ord = readInt();
        return enumClass.getEnumConstants()[ord];
    }

    ByteBuf readByteBuf() throws IOException;

    <T extends Streamable> T readStreamable() throws IOException;

    default <T extends Streamable> T readStreamable(StreamableReader<T> reader) throws IOException {
        return reader.read(this);
    }

    default <T extends Streamable> T readStreamableOrNull(StreamableReader<T> reader) throws IOException {
        if (readBoolean()) {
            return reader.read(this);
        } else {
            return null;
        }
    }

    default <T extends Streamable> ImmutableList<T> readStreamableList(StreamableReader<T> reader) throws IOException {
        int size = readInt();
        if (size == 0) {
            return ImmutableList.of();
        } else if (size == 1) {
            return ImmutableList.of(reader.read(this));
        } else {
            ImmutableList.Builder<T> builder = ImmutableList.builder();
            for (int i = 0; i < size; i++) {
                builder.add(reader.read(this));
            }
            return builder.build();
        }
    }

    default <T extends Streamable> ImmutableSet<T> readStreamableSet(StreamableReader<T> reader) throws IOException {
        int size = readInt();
        if (size == 0) {
            return ImmutableSet.of();
        } else if (size == 1) {
            return ImmutableSet.of(reader.read(this));
        } else {
            ImmutableSet.Builder<T> builder = ImmutableSet.builder();
            for (int i = 0; i < size; i++) {
                builder.add(reader.read(this));
            }
            return builder.build();
        }
    }
}
