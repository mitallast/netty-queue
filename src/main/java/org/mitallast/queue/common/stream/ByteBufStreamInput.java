package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;

public class ByteBufStreamInput extends ByteBufInputStream implements StreamInput {

    private final StreamableClassRegistry classRegistry;
    private final ByteBuf buffer;

    public ByteBufStreamInput(StreamableClassRegistry classRegistry, ByteBuf buffer) {
        this(classRegistry, buffer, buffer.readableBytes());
    }

    public ByteBufStreamInput(StreamableClassRegistry classRegistry, ByteBuf buffer, int length) {
        super(buffer, length);
        this.classRegistry = classRegistry;
        this.buffer = buffer;
    }

    @Override
    public String readText() throws IOException {
        return readUTF();
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
    public <T extends Streamable> T readStreamable() throws IOException {
        return classRegistry.readStreamable(this);
    }
}
