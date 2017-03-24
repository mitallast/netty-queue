package org.mitallast.queue.common.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

@Deprecated
public interface StreamInput extends DataInput, Closeable {

    int available() throws IOException;

    @Override
    void readFully(byte[] b) throws IOException;

    @Override
    void readFully(byte[] b, int off, int len) throws IOException;

    @Override
    int skipBytes(int n) throws IOException;

    @Override
    boolean readBoolean() throws IOException;

    @Override
    byte readByte() throws IOException;

    @Override
    int readUnsignedByte() throws IOException;

    @Override
    short readShort() throws IOException;

    @Override
    int readUnsignedShort() throws IOException;

    @Override
    char readChar() throws IOException;

    @Override
    int readInt() throws IOException;

    @Override
    long readLong() throws IOException;

    @Override
    float readFloat() throws IOException;

    @Override
    double readDouble() throws IOException;

    default String readText() throws IOException {
        return readUTF();
    }

    default String readTextOrNull() throws IOException {
        if (readBoolean()) {
            return readUTF();
        } else {
            return null;
        }
    }

    default UUID readUUID() throws IOException {
        long most = readLong();
        long least = readLong();
        return new UUID(most, least);
    }

    default UUID readUUIDOrNull() throws IOException {
        long most = readLong();
        long least = readLong();
        if (most == 0 && least == 0) {
            return null;
        } else {
            return new UUID(most, least);
        }
    }

    default <Type extends Enum<Type>> Type readEnum(Class<Type> enumClass) throws IOException {
        int ord = readInt();
        return enumClass.getEnumConstants()[ord];
    }

    default <Type extends Enum<Type>> Type readEnumOrNull(Class<Type> enumClass) throws IOException {
        int ord = readInt();
        if (ord < 0) {
            return null;
        } else {
            return enumClass.getEnumConstants()[ord];
        }
    }

    ByteBuf readByteBuf() throws IOException;

    ByteBuf readByteBufOrNull() throws IOException;

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
