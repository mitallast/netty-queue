package org.mitallast.queue.common.codec;

import javaslang.Function1;
import javaslang.Function3;

import java.io.DataInput;
import java.io.DataOutput;

final class Codec3<Type extends Message, Param1, Param2, Param3> implements Codec<Type> {
    private final Function3<Param1, Param2, Param3, Type> builder;
    private final Function1<Type, Param1> lens1;
    private final Function1<Type, Param2> lens2;
    private final Function1<Type, Param3> lens3;
    private final Codec<Param1> codec1;
    private final Codec<Param2> codec2;
    private final Codec<Param3> codec3;

    public Codec3(
        Function3<Param1, Param2, Param3, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Function1<Type, Param3> lens3,
        Codec<Param1> codec1,
        Codec<Param2> codec2,
        Codec<Param3> codec3
    ) {
        this.builder = builder;
        this.lens1 = lens1;
        this.lens2 = lens2;
        this.lens3 = lens3;
        this.codec1 = codec1;
        this.codec2 = codec2;
        this.codec3 = codec3;
    }

    @Override
    public Type read(DataInput stream) {
        Param1 param1 = codec1.read(stream);
        Param2 param2 = codec2.read(stream);
        Param3 param3 = codec3.read(stream);
        return builder.apply(param1, param2, param3);
    }

    @Override
    public void write(DataOutput stream, Type value) {
        codec1.write(stream, lens1.apply(value));
        codec2.write(stream, lens2.apply(value));
        codec3.write(stream, lens3.apply(value));
    }
}
