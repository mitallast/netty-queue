package org.mitallast.queue.common.codec;

import javaslang.Function1;

import java.io.DataInput;
import java.io.DataOutput;

final class Codec1<Type extends Message, Param1> implements Codec<Type> {
    private final Function1<Param1, Type> builder;
    private final Function1<Type, Param1> lens1;
    private final Codec<Param1> codec1;

    public Codec1(
        Function1<Param1, Type> builder,
        Function1<Type, Param1> lens1,
        Codec<Param1> codec1
    ) {
        this.builder = builder;
        this.lens1 = lens1;
        this.codec1 = codec1;
    }

    @Override
    public Type read(DataInput stream) {
        Param1 param1 = codec1.read(stream);
        return builder.apply(param1);
    }

    @Override
    public void write(DataOutput stream, Type value) {
        codec1.write(stream, lens1.apply(value));
    }
}
