package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;

public class DataStreamInput extends StreamInput {
    private final InputStream input;

    public DataStreamInput(StreamableClassRegistry classRegistry, InputStream input) {
        super(classRegistry);
        this.input = input;
    }

    @Override
    public int available() throws IOException {
        return input.available();
    }

    @Override
    public int read() throws IOException {
        return input.read();
    }

    @Override
    public void read(byte[] b, int off, int len) throws IOException {
        if (input.read(b, off, len) != len) {
            throw new IOException("Unexpected EOF");
        }
    }

    @Override
    public void skipBytes(int n) throws IOException {
        if (input.skip(n) != n) {
            throw new IOException("Unexpected EOF");
        }
    }

    @Override
    public ByteBuf readByteBuf() throws IOException {
        int size = readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        byte[] bytes = new byte[size];
        int read = input.read(bytes);
        if (read != size) {
            throw new IOException("error read: got " + read + " bytes, expected " + size);
        }
        return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
