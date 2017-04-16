package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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
    public int available() {
        try {
            return input.available();
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public int read() {
        try {
            return input.read();
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public void read(byte[] b, int off, int len) {
        try {
            if (input.read(b, off, len) != len) {
                throw new StreamException("Unexpected EOF");
            }
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public void skipBytes(int n) {
        try {
            if (input.skip(n) != n) {
                throw new StreamException("Unexpected EOF");
            }
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public ByteBuf readByteBuf() {
        int size = readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(size);
        try {
            buffer.writeBytes(input, size);
        } catch (IOException e) {
            throw new StreamException(e);
        }
        return buffer;
    }

    @Override
    public void close() {
        try {
            input.close();
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }
}
