package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DataStreamOutput extends DataOutputStream implements StreamOutput {

    private final StreamableClassRegistry classRegistry;
    private final OutputStream output;

    public DataStreamOutput(StreamableClassRegistry classRegistry, OutputStream output) {
        super(output);
        this.classRegistry = classRegistry;
        this.output = output;
    }

    @Override
    public void write(int b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
    }

    @Override
    public void writeByteBuf(ByteBuf buffer) throws IOException {
        writeByteBuf(buffer, buffer.readableBytes());
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) throws IOException {
        writeInt(length);
        if (length > 0) {
            buffer.readBytes(this, length);
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer) throws IOException {
        if (buffer == null) {
            writeInt(-1);
        } else {
            writeByteBuf(buffer);
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer, int length) throws IOException {
        if (buffer == null) {
            writeInt(-1);
        } else {
            writeByteBuf(buffer, length);
        }
    }

    @Override
    public <T extends Streamable> void writeClass(Class<T> streamableClass) throws IOException {
        classRegistry.writeClass(this, streamableClass);
    }

    @Override
    public void close() throws IOException {
        output.flush();
        output.close();
    }
}
