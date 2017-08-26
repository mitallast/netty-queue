package org.mitallast.queue.common.codec;

import javaslang.collection.Seq;
import javaslang.collection.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class SeqCodec<Type> implements Codec<Seq<Type>> {
    private final Codec<Type> codec;

    SeqCodec(Codec<Type> codec) {
        this.codec = codec;
    }

    @Override
    public Seq<Type> read(DataInput stream) {
        try {
            int size = stream.readInt();
            return Vector.fill(size, () -> codec.read(stream));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Seq<Type> set) {
        try {
            stream.writeInt(set.size());
            set.forEach(i -> codec.write(stream, i));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
