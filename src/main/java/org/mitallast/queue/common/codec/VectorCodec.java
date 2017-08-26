package org.mitallast.queue.common.codec;

import javaslang.collection.Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class VectorCodec<Type> implements Codec<Vector<Type>> {
    private final Codec<Type> codec;

    VectorCodec(Codec<Type> codec) {
        this.codec = codec;
    }

    @Override
    public Vector<Type> read(DataInput stream) {
        try {
            int size = stream.readInt();
            return Vector.fill(size, () -> codec.read(stream));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Vector<Type> vector) {
        try {
            stream.writeInt(vector.size());
            vector.forEach(i -> codec.write(stream, i));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
