package org.mitallast.queue.common.codec;

import javaslang.Function1;
import javaslang.Function2;

import java.io.DataInput;
import java.io.DataOutput;

final class Codec2<Type extends Message, Param1, Param2> implements Codec<Type> {
    private final Function2<Param1, Param2, Type> builder;
    private final Function1<Type, Param1> lens1;
    private final Function1<Type, Param2> lens2;
    private final Codec<Param1> codec1;
    private final Codec<Param2> codec2;

    public Codec2(
        Function2<Param1, Param2, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Codec<Param1> codec1,
        Codec<Param2> codec2
    ) {
        this.builder = builder;
        this.lens1 = lens1;
        this.lens2 = lens2;
        this.codec1 = codec1;
        this.codec2 = codec2;
    }

    @Override
    public Type read(DataInput stream) {
        Param1 param1 = codec1.read(stream);
        Param2 param2 = codec2.read(stream);
        return builder.apply(param1, param2);
    }

    @Override
    public void write(DataOutput stream, Type value) {
        codec1.write(stream, lens1.apply(value));
        codec2.write(stream, lens2.apply(value));
    }
}
