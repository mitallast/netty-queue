package org.mitallast.queue.common.codec;

import javaslang.collection.HashSet;
import javaslang.collection.Set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class SetCodec<Type> implements Codec<Set<Type>> {
    private final Codec<Type> codec;

    SetCodec(Codec<Type> codec) {
        this.codec = codec;
    }

    @Override
    public Set<Type> read(DataInput stream) {
        try {
            int size = stream.readInt();
            return HashSet.fill(size, () -> codec.read(stream));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Set<Type> set) {
        try {
            stream.writeInt(set.size());
            set.forEach(i -> codec.write(stream, i));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
