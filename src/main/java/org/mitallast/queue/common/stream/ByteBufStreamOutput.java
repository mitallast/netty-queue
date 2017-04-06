package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class ByteBufStreamOutput extends StreamOutput {
    private final ByteBuf buffer;

    public ByteBufStreamOutput(StreamableClassRegistry classRegistry, ByteBuf buffer) {
        super(classRegistry);
        this.buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException {
        buffer.writeByte(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        buffer.writeBytes(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        buffer.writeBoolean(v);
    }

    @Override
    public void writeUnsignedShort(int v) throws IOException {
        buffer.writeShort(v);
    }

    @Override
    public void writeShort(short v) throws IOException {
        buffer.writeShort(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        buffer.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        buffer.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        buffer.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        buffer.writeDouble(v);
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) throws IOException {
        writeInt(length);
        if (length > 0) {
            this.buffer.ensureWritable(length);
            buffer.readBytes(this.buffer, length);
        }
    }

    @Override
    public void flush() throws IOException {}

    @Override
    public void close() throws IOException {}
}
