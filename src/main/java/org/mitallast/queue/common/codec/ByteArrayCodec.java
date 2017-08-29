package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

public class ByteArrayCodec implements Codec<byte[]> {
    private final static byte[] empty = new byte[0];

    @Override
    public byte[] read(DataInput stream) {
        try {
            int size = stream.readInt();
            if (size > 0) {
                byte[] data = new byte[size];
                stream.readFully(data);
                return data;
            } else {
                return empty;
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, byte[] value) {
        try {
            stream.writeInt(value.length);
            if (value.length > 0) {
                stream.write(value);
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
