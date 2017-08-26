package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

class IntCodec implements Codec<Integer> {
    @Override
    public Integer read(DataInput stream) {
        try {
            return stream.readInt();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Integer value) {
        try {
            stream.writeInt(value);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
