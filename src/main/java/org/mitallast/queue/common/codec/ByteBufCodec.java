package org.mitallast.queue.common.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class ByteBufCodec implements Codec<ByteBuf> {

    @Override
    public ByteBuf read(DataInput stream) {
        ByteBuf buffer = null;
        try {
            int size = stream.readInt();
            if (size == 0) {
                return Unpooled.EMPTY_BUFFER;
            }
            buffer = PooledByteBufAllocator.DEFAULT.buffer(size);
            int max = 64;
            byte[] buf = new byte[Math.min(size, max)];
            for (int i = 0; i < size; ) {
                int l = Math.min(max, size - i);
                stream.readFully(buf, 0, l);
                buffer.writeBytes(buf, 0, l);
                i += l;
            }
            return buffer;
        } catch (IOException e) {
            if (buffer != null) {
                buffer.release();
            }
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, ByteBuf value) {
        try {
            int size = value.readableBytes();
            stream.writeInt(size);
            if (size > 0) {
                int max = 64;
                byte[] buffer = new byte[Math.min(size, max)];
                for (int i = 0; i < size; ) {
                    int l = Math.min(max, size - i);
                    value.readBytes(buffer, 0, l);
                    stream.write(buffer, 0, l);
                    i += l;
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
