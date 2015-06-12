package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

public class ByteBufStreamOutput extends ByteBufOutputStream implements StreamOutput {

    private final ByteBuf buffer;

    public ByteBufStreamOutput(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
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
    public void writeByteBufOrNull(ByteBuf buffer) throws IOException {
        if (buffer == null) {
            writeInt(-1);
        } else {
            int length = buffer.readableBytes();
            writeInt(length);
            if (length > 0) {
                this.buffer.ensureWritable(length);
                buffer.readBytes(this.buffer, length);
            }
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer, int length) throws IOException {
        if (buffer == null) {
            writeInt(-1);
        } else {
            writeInt(length);
            if (length > 0) {
                this.buffer.ensureWritable(length);
                buffer.readBytes(this.buffer, length);
            }
        }
    }
}
