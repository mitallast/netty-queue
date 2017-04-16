package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import javaslang.collection.HashSet;
import javaslang.collection.Seq;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import javaslang.control.Option;

import java.io.Closeable;

public abstract class StreamInput implements Closeable {
    private final StreamableClassRegistry classRegistry;
    private byte[] buffer = new byte[64];

    protected StreamInput(StreamableClassRegistry classRegistry) {
        this.classRegistry = classRegistry;
    }

    public abstract int available();

    public abstract int read();

    public final void read(byte[] b) {
        read(b, 0, b.length);
    }

    public abstract void read(byte[] b, int off, int len);

    public abstract void skipBytes(int n);

    public boolean readBoolean() {
        return read() == 1;
    }

    public byte readByte() {
        return (byte) read();
    }

    public short readShort() {
        return (short) readUnsignedShort();
    }

    public int readUnsignedShort() {
        read(buffer, 0, 2);
        return ((int) buffer[0] & 0xff) << 8 | (int) buffer[1] & 0xff;
    }

    public int readInt() {
        read(buffer, 0, 4);
        return ((int) buffer[0] & 0xff) << 24 |
            ((int) buffer[1] & 0xff) << 16 |
            ((int) buffer[2] & 0xff) << 8 |
            (int) buffer[3] & 0xff;
    }

    public long readLong() {
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

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public final String readText() {
        int bytes = readUnsignedShort();
        if (buffer == null || buffer.length < bytes) {
            buffer = new byte[bytes];
        }
        read(buffer, 0, bytes);
        return new String(buffer, 0, bytes, CharsetUtil.UTF_8).intern();
    }

    public final <Type extends Enum<Type>> Type readEnum(Class<Type> enumClass) {
        int ord = readUnsignedShort();
        return enumClass.getEnumConstants()[ord];
    }

    public abstract ByteBuf readByteBuf();

    public final <T extends Streamable> T readStreamable() {
        return classRegistry.readStreamable(this);
    }

    public final <T extends Streamable> T readStreamable(StreamableReader<T> reader) {
        return reader.read(this);
    }

    public final Vector<Streamable> readVector() {
        int size = readInt();
        return Vector.fill(size, this::readStreamable);
    }

    public final <T extends Streamable> Vector<T> readVector(StreamableReader<T> reader) {
        int size = readInt();
        return Vector.fill(size, () -> readStreamable(reader));
    }

    public final <T extends Streamable> Set<T> readSet(StreamableReader<T> reader) {
        int size = readInt();
        return HashSet.fill(size, () -> readStreamable(reader));
    }

    public final <T extends Streamable> Seq<T> readSeq(StreamableReader<T> reader) {
        int size = readInt();
        return Vector.fill(size, () -> readStreamable(reader));
    }

    public final <T extends Streamable> Option<T> readOpt(StreamableReader<T> reader) {
        if (readBoolean()) {
            return Option.some(readStreamable(reader));
        } else {
            return Option.none();
        }
    }

    @Override
    public abstract void close();
}
