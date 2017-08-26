package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class StringCodec implements Codec<String> {
    @Override
    public String read(DataInput stream) {
        try {
            return stream.readUTF();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, String value) {
        try {
            stream.writeUTF(value);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
