package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

class BooleanCodec implements Codec<Boolean> {
    @Override
    public Boolean read(DataInput stream) {
        try {
            return stream.readBoolean();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Boolean value) {
        try {
            stream.writeBoolean(value);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
