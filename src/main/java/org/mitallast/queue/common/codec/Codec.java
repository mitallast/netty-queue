package org.mitallast.queue.common.codec;

import io.netty.buffer.ByteBuf;
import javaslang.*;
import javaslang.collection.Seq;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import javaslang.control.Option;

import java.io.DataInput;
import java.io.DataOutput;

public interface Codec<T> {

    T read(DataInput stream);

    void write(DataOutput stream, T value);

    default Codec<Option<T>> opt() {
        return optionCodec(this);
    }

    Codec<Boolean> booleanCodec = new BooleanCodec();
    Codec<Integer> intCodec = new IntCodec();
    Codec<Long> longCodec = new LongCodec();
    Codec<String> stringCodec = new StringCodec();
    Codec<ByteBuf> bufCodec = new ByteBufCodec();

    static <T extends Message> void register(int code, Class<T> type, Codec<T> codec) {
        AnyCodec.register(code, type, codec);
    }

    @SuppressWarnings("unchecked")
    static <T extends Message> Codec<T> anyCodec() {
        return (Codec<T>) new AnyCodec();
    }

    static <T extends Enum<T>> Codec<T> enumCodec(Class<T> enumClass) {
        return new EnumCodec<>(enumClass);
    }

    static <T> Codec<Vector<T>> vectorCodec(Codec<T> codec) {
        return new VectorCodec<>(codec);
    }

    static <T> Codec<Set<T>> setCodec(Codec<T> codec) {
        return new SetCodec<>(codec);
    }

    static <T> Codec<Seq<T>> seqCodec(Codec<T> codec) {
        return new SeqCodec<>(codec);
    }

    static <T> Codec<Option<T>> optionCodec(Codec<T> codec) {
        return new OptionCodec<>(codec);
    }

    static <Type extends Message> Codec<Type> of(Type value) {
        return new StaticCodec<>(value);
    }

    static <Type extends Message, Param1> Codec<Type> of(
        Function1<Param1, Type> builder,
        Function1<Type, Param1> lens1,
        Codec<Param1> codec1
    ) {
        return new Codec1<>(builder, lens1, codec1);
    }

    static <Type extends Message, Param1, Param2> Codec<Type> of(
        Function2<Param1, Param2, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Codec<Param1> codec1,
        Codec<Param2> codec2
    ) {
        return new Codec2<>(builder, lens1, lens2, codec1, codec2);
    }

    static <Type extends Message, Param1, Param2, Param3> Codec<Type> of(
        Function3<Param1, Param2, Param3, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Function1<Type, Param3> lens3,
        Codec<Param1> codec1,
        Codec<Param2> codec2,
        Codec<Param3> codec3
    ) {
        return new Codec3<>(builder, lens1, lens2, lens3, codec1, codec2, codec3);
    }

    static <Type extends Message, Param1, Param2, Param3, Param4> Codec<Type> of(
        Function4<Param1, Param2, Param3, Param4, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Function1<Type, Param3> lens3,
        Function1<Type, Param4> lens4,
        Codec<Param1> codec1,
        Codec<Param2> codec2,
        Codec<Param3> codec3,
        Codec<Param4> codec4
    ) {
        return new Codec4<>(builder, lens1, lens2, lens3, lens4, codec1, codec2, codec3, codec4);
    }

    static <Type extends Message, Param1, Param2, Param3, Param4, Param5> Codec<Type> of(
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
        return new Codec5<>(builder,
            lens1, lens2, lens3, lens4, lens5,
            codec1, codec2, codec3, codec4, codec5);
    }

    static <Type extends Message, Param1, Param2, Param3, Param4, Param5, Param6> Codec<Type> of(
        Function6<Param1, Param2, Param3, Param4, Param5, Param6, Type> builder,
        Function1<Type, Param1> lens1,
        Function1<Type, Param2> lens2,
        Function1<Type, Param3> lens3,
        Function1<Type, Param4> lens4,
        Function1<Type, Param5> lens5,
        Function1<Type, Param6> lens6,
        Codec<Param1> codec1,
        Codec<Param2> codec2,
        Codec<Param3> codec3,
        Codec<Param4> codec4,
        Codec<Param5> codec5,
        Codec<Param6> codec6
    ) {
        return new Codec6<>(builder,
            lens1, lens2, lens3, lens4, lens5, lens6,
            codec1, codec2, codec3, codec4, codec5, codec6);
    }
}