package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class LongCodec implements Codec<Long> {
    @Override
    public Long read(DataInput stream) {
        try {
            return stream.readLong();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Long value) {
        try {
            stream.writeLong(value);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
