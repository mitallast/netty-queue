package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;

public class DataStreamInput implements StreamInput {

    private final StreamableClassRegistry classRegistry;
    private final DataInput input;

    public DataStreamInput(StreamableClassRegistry classRegistry, DataInput input) {
        this.classRegistry = classRegistry;
        this.input = input;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

    @Override
    public ByteBuf readByteBuf() throws IOException {
        int size = readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public ByteBuf readByteBufOrNull() throws IOException {
        int size = readInt();
        if (size == 0) {
            return null;
        }
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public <T extends Streamable> T readStreamable() throws IOException {
        return classRegistry.readStreamable(this);
    }

    @Override
    public void close() throws IOException {
        if (input instanceof Closeable) {
            ((Closeable) input).close();
        }
    }
}
