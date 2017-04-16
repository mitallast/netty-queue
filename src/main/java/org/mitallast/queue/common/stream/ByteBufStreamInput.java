package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ByteBufStreamInput extends StreamInput {
    private final ByteBuf buffer;

    public ByteBufStreamInput(StreamableClassRegistry classRegistry, ByteBuf buffer) {
        super(classRegistry);
        this.buffer = buffer;
    }

    @Override
    public int available() {
        return buffer.readableBytes();
    }

    @Override
    public int read() {
        return buffer.readUnsignedByte();
    }

    @Override
    public void read(byte[] b, int off, int len) {
        buffer.readBytes(b, off, len);
    }

    @Override
    public void skipBytes(int n) {
        buffer.skipBytes(n);
    }

    @Override
    public boolean readBoolean() {
        return buffer.readBoolean();
    }

    @Override
    public byte readByte() {
        return buffer.readByte();
    }

    @Override
    public short readShort() {
        return buffer.readShort();
    }

    @Override
    public int readUnsignedShort() {
        return buffer.readUnsignedShort();
    }

    @Override
    public int readInt() {
        return buffer.readInt();
    }

    @Override
    public long readLong() {
        return buffer.readLong();
    }

    @Override
    public float readFloat() {
        return buffer.readFloat();
    }

    @Override
    public double readDouble() {
        return buffer.readDouble();
    }

    @Override
    public ByteBuf readByteBuf() {
        int size = readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        return buffer.readSlice(size).retain();
    }

    @Override
    public void close() {}
}
