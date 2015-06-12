package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;

public class ByteBufStreamInput extends ByteBufInputStream implements StreamInput {

    private final ByteBuf buffer;

    public ByteBufStreamInput(ByteBuf buffer) {
        this(buffer, buffer.readableBytes());
    }

    public ByteBufStreamInput(ByteBuf buffer, int length) {
        super(buffer, length);
        this.buffer = buffer;
    }

    @Override
    public ByteBuf readByteBuf() throws IOException {
        int size = buffer.readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        return buffer.readSlice(size).retain();
    }

    @Override
    public ByteBuf readByteBufOrNull() throws IOException {
        int size = buffer.readInt();
        if (size <= 0) {
            return null;
        }
        return buffer.readSlice(size).retain();
    }
}
