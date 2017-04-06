package org.mitallast.queue.common.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.io.Closeable;
import java.io.IOException;

public abstract class StreamInput implements Closeable {
    private final StreamableClassRegistry classRegistry;
    private byte[] buffer = new byte[64];

    protected StreamInput(StreamableClassRegistry classRegistry) {
        this.classRegistry = classRegistry;
    }

    public abstract int available() throws IOException;

    public abstract int read() throws IOException;

    public final void read(byte[] b) throws IOException {
        read(b, 0, b.length);
    }

    public abstract void read(byte[] b, int off, int len) throws IOException;

    public abstract void skipBytes(int n) throws IOException;

    public boolean readBoolean() throws IOException {
        return read() == 1;
    }

    public byte readByte() throws IOException {
        return (byte) read();
    }

    public short readShort() throws IOException {
        return (short) readUnsignedShort();
    }

    public int readUnsignedShort() throws IOException {
        read(buffer, 0, 2);
        return ((int) buffer[0] & 0xff) << 8 | (int) buffer[1] & 0xff;
    }

    public int readInt() throws IOException {
        read(buffer, 0, 4);
        return ((int) buffer[0] & 0xff) << 24 |
            ((int) buffer[1] & 0xff) << 16 |
            ((int) buffer[2] & 0xff) << 8 |
            (int) buffer[3] & 0xff;
    }

    public long readLong() throws IOException {
        read(buffer, 0, 8);
        return ((long) buffer[0] & 0xff) << 56 |
            ((long) buffer[1] & 0xff) << 48 |
            ((long) buffer[2] & 0xff) << 40 |
            ((long) buffer[3] & 0xff) << 32 |
            ((long) buffer[4] & 0xff) << 24 |
            ((long) buffer[5] & 0xff) << 16 |
            ((long) buffer[6] & 0xff) << 8 |
            (long) buffer[7] & 0xff;
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public final String readText() throws IOException {
        int bytes = readUnsignedShort();
        if (buffer == null || buffer.length < bytes) {
            buffer = new byte[bytes];
        }
        read(buffer, 0, bytes);
        return new String(buffer, 0, bytes, CharsetUtil.UTF_8);
    }

    public final <Type extends Enum<Type>> Type readEnum(Class<Type> enumClass) throws IOException {
        int ord = readUnsignedShort();
        return enumClass.getEnumConstants()[ord];
    }

    public abstract ByteBuf readByteBuf() throws IOException;

    public final <T extends Streamable> T readStreamable() throws IOException {
        return classRegistry.readStreamable(this);
    }

    public final <T extends Streamable> T readStreamable(StreamableReader<T> reader) throws IOException {
        return reader.read(this);
    }

    public final <T extends Streamable> T readStreamableOrNull(StreamableReader<T> reader) throws IOException {
        if (readBoolean()) {
            return reader.read(this);
        } else {
            return null;
        }
    }

    public final <T extends Streamable> ImmutableList<T> readStreamableList(StreamableReader<T> reader) throws IOException {
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

    public final <T extends Streamable> ImmutableSet<T> readStreamableSet(StreamableReader<T> reader) throws IOException {
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
