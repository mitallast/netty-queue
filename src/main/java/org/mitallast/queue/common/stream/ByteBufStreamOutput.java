package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

public class ByteBufStreamOutput extends ByteBufOutputStream implements StreamOutput {

    private final StreamableClassRegistry classRegistry;
    private final ByteBuf buffer;

    public ByteBufStreamOutput(StreamableClassRegistry classRegistry, ByteBuf buffer) {
        super(buffer);
        this.classRegistry = classRegistry;
        this.buffer = buffer;
    }

    @Override
    public void writeBytes(String string) throws IOException {
        if (buffer.hasArray()) {
            int length = string.length();
            buffer.ensureWritable(length);
            byte[] array = buffer.array();
            int offset = buffer.arrayOffset() + buffer.writerIndex();
            for (int i = 0; i < length; i++) {
                array[offset + i] = (byte) string.charAt(i);
            }
            buffer.writerIndex(buffer.writerIndex() + length);
        } else {
            super.writeBytes(string);
        }
    }

    @Override
    public void writeByteBuf(ByteBuf buffer) throws IOException {
        int length = buffer.readableBytes();
        writeInt(length);
        if (length > 0) {
            this.buffer.ensureWritable(length);
            buffer.readBytes(this.buffer, length);
        }
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
    public <T extends Streamable> void writeClass(Class<T> streamableClass) throws IOException {
        classRegistry.writeClass(this, streamableClass);
    }
}
