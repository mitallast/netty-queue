package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface StreamOutput extends DataOutput, Closeable, Flushable {

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

    default <Type extends Enum<Type>> void writeEnum(Type type) throws IOException {
        writeInt(type.ordinal());
    }

    void writeByteBuf(ByteBuf buffer) throws IOException;

    void writeByteBuf(ByteBuf buffer, int length) throws IOException;

    <T extends Streamable> void writeClass(Class<T> streamableClass) throws IOException;

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
}
