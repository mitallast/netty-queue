package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

public class ByteBufStreamInput extends StreamInput {
    private final ByteBuf buffer;

    public ByteBufStreamInput(StreamableClassRegistry classRegistry, ByteBuf buffer) {
        super(classRegistry);
        this.buffer = buffer;
    }

    @Override
    public int available() throws IOException {
        return buffer.readableBytes();
    }

    @Override
    public int read() throws IOException {
        return buffer.readUnsignedByte();
    }

    @Override
    public void read(byte[] b, int off, int len) throws IOException {
        buffer.readBytes(b, off, len);
    }

    @Override
    public void skipBytes(int n) throws IOException {
        buffer.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return buffer.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return buffer.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return buffer.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return buffer.readUnsignedShort();
    }

    @Override
    public int readInt() throws IOException {
        return buffer.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return buffer.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return buffer.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return buffer.readDouble();
    }

    @Override
    public ByteBuf readByteBuf() throws IOException {
        int size = readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        return buffer.readSlice(size).retain();
    }

    @Override
    public void close() throws IOException {}
}
