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
    public void write(int b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) throws IOException {
        buffer.readBytes(output, length);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

    @Override
    public void flush() throws IOException {
        output.flush();
    }
}
