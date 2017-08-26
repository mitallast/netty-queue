package org.mitallast.queue.common.codec;

import javaslang.Function1;
import javaslang.Function5;

import java.io.DataInput;
import java.io.DataOutput;

final class Codec5<Type extends Message, Param1, Param2, Param3, Param4, Param5> implements Codec<Type> {
    private final Function5<Param1, Param2, Param3, Param4, Param5, Type> builder;
    private final Function1<Type, Param1> lens1;
    private final Function1<Type, Param2> lens2;
    private final Function1<Type, Param3> lens3;
    private final Function1<Type, Param4> lens4;
    private final Function1<Type, Param5> lens5;
    private final Codec<Param1> codec1;
    private final Codec<Param2> codec2;
    private final Codec<Param3> codec3;
    private final Codec<Param4> codec4;
    private final Codec<Param5> codec5;

    public Codec5(
        Function5<Param1, Param2, Param3, Param4, Param5, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Function1<Type, Param3> lens3,
        Function1<Type, Param4> lens4,
        Function1<Type, Param5> lens5,
        Codec<Param1> codec1,
        Codec<Param2> codec2,
        Codec<Param3> codec3,
        Codec<Param4> codec4,
        Codec<Param5> codec5
    ) {
        this.builder = builder;
        this.lens1 = lens1;
        this.lens2 = lens2;
        this.lens3 = lens3;
        this.lens4 = lens4;
        this.lens5 = lens5;
        this.codec1 = codec1;
        this.codec2 = codec2;
        this.codec3 = codec3;
        this.codec4 = codec4;
        this.codec5 = codec5;
    }

    @Override
    public Type read(DataInput stream) {
        Param1 param1 = codec1.read(stream);
        Param2 param2 = codec2.read(stream);
        Param3 param3 = codec3.read(stream);
        Param4 param4 = codec4.read(stream);
        Param5 param5 = codec5.read(stream);
        return builder.apply(param1, param2, param3, param4, param5);
    }

    @Override
    public void write(DataOutput stream, Type value) {
        codec1.write(stream, lens1.apply(value));
        codec2.write(stream, lens2.apply(value));
        codec3.write(stream, lens3.apply(value));
        codec4.write(stream, lens4.apply(value));
        codec5.write(stream, lens5.apply(value));
    }
}
