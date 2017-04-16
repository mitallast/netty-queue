package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

public class ByteBufStreamOutput extends StreamOutput {
    private final ByteBuf buffer;

    public ByteBufStreamOutput(StreamableClassRegistry classRegistry, ByteBuf buffer) {
        super(classRegistry);
        this.buffer = buffer;
    }

    @Override
    public void write(int b) {
        buffer.writeByte(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        buffer.writeBytes(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) {
        buffer.writeBoolean(v);
    }

    @Override
    public void writeUnsignedShort(int v) {
        buffer.writeShort(v);
    }

    @Override
    public void writeShort(short v) {
        buffer.writeShort(v);
    }

    @Override
    public void writeInt(int v) {
        buffer.writeInt(v);
    }

    @Override
    public void writeLong(long v) {
        buffer.writeLong(v);
    }

    @Override
    public void writeFloat(float v) {
        buffer.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) {
        buffer.writeDouble(v);
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) {
        writeInt(length);
        if (length > 0) {
            buffer.readBytes(this.buffer, length);
        }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
}
