package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;

public class DataStreamOutput extends StreamOutput {

    private final OutputStream output;

    public DataStreamOutput(StreamableClassRegistry classRegistry, OutputStream output) {
        super(classRegistry);
        this.output = output;
    }

    @Override
    public void write(int b) {
        try {
            output.write(b);
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) {
        try {
            output.write(b, off, len);
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) {
        writeInt(length);
        try {
            buffer.readBytes(output, length);
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public void close() {
        try {
            output.close();
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    @Override
    public void flush() {
        try {
            output.flush();
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }
}
