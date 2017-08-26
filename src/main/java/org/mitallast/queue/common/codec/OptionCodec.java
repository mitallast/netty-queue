package org.mitallast.queue.common.codec;

import javaslang.control.Option;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class OptionCodec<Type> implements Codec<Option<Type>> {
    private final Codec<Type> codec;

    OptionCodec(Codec<Type> codec) {
        this.codec = codec;
    }

    @Override
    public Option<Type> read(DataInput stream) {
        try {
            if (stream.readBoolean()) {
                return Option.of(codec.read(stream));
            } else {
                return Option.none();
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Option<Type> option) {
        try {
            stream.writeBoolean(option.isDefined());
            option.forEach(i -> codec.write(stream, i));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
