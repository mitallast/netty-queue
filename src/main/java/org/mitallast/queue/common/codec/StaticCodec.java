package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;

class StaticCodec<T extends Message> implements Codec<T> {
    private final T value;

    StaticCodec(T value) {
        this.value = value;
    }

    @Override
    public T read(DataInput stream) {
        return value;
    }

    @Override
    public void write(DataOutput stream, T value) {}
}
