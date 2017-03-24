package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

@Deprecated
public class DataStreamInput extends DataInputStream implements StreamInput {

    private final StreamableClassRegistry classRegistry;
    private final InputStream input;

    public DataStreamInput(StreamableClassRegistry classRegistry, InputStream input) {
        super(input);
        this.classRegistry = classRegistry;
        this.input = input;
    }

    public int available() throws IOException {
        return input.available();
    }

    @Override
    public ByteBuf readByteBuf() throws IOException {
        int size = readInt();
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        byte[] bytes = new byte[size];
        input.read(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public ByteBuf readByteBufOrNull() throws IOException {
        int size = readInt();
        if (size == 0) {
            return null;
        }
        byte[] bytes = new byte[size];
        input.read(bytes);
        return Unpooled.wrappedBuffer(bytes);
    }

    @Override
    public <T extends Streamable> T readStreamable() throws IOException {
        return classRegistry.readStreamable(this);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
