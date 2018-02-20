package org.mitallast.queue.common.codec

import com.google.common.base.Preconditions
import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.Unpooled
import io.vavr.collection.HashSet
import io.vavr.collection.Seq
import io.vavr.collection.Set
import io.vavr.collection.Vector
import io.vavr.control.Option
import java.io.DataInput
import java.io.DataOutput

interface Message

interface Codec<T> {
    fun read(stream: DataInput): T

    fun write(stream: DataOutput, value: T)

//    fun opt(): Codec<Option<T>> = Codec.optionCodec(this)
//    fun vector(): Codec<Vector<T>> = Codec.vectorCodec(this)
//    fun set(): Codec<Set<T>> = Codec.setCodec(this)
//    fun seq(): Codec<Seq<T>> = Codec.seqCodec(this)

    companion object {

        fun <T : Message> register(code: Int, type: Class<T>, codec: Codec<T>) {
            AnyCodec.register(code, type, codec)
        }

        fun booleanCodec(): Codec<Boolean> = BooleanCodec
        fun intCodec(): Codec<Int> = IntCodec
        fun longCodec(): Codec<Long> = LongCodec
        fun stringCodec(): Codec<String> = StringCodec
        fun bytesCodec(): Codec<ByteArray> = ByteArrayCodec
        fun byteBufCodec(): Codec<ByteBuf> = ByteBufCodec

        fun <T : Message> anyCodec(): Codec<T> = AnyCodec()
        fun <T : Enum<T>> enumCodec(enumClass: Class<T>): Codec<T> = EnumCodec(enumClass)
        fun <T> optionCodec(codec: Codec<T>): Codec<Option<T>> = OptionCodec(codec)
        fun <T> vectorCodec(codec: Codec<T>): Codec<Vector<T>> = VectorCodec(codec)
        fun <T> setCodec(codec: Codec<T>): Codec<Set<T>> = SetCodec(codec)
        fun <T> seqCodec(codec: Codec<T>): Codec<Seq<T>> = SeqCodec(codec)

        fun <T> of(value: T): Codec<T> = StaticCodec(value)

        fun <Type, Param1> of(
                builder: Function1<Param1, Type>,
                lens1: Function1<Type, Param1>,
                codec1: Codec<Param1>
        ): Codec<Type> {
            return Codec1(builder, lens1, codec1)
        }

        fun <Type, Param1, Param2> of(
                builder: Function2<Param1, Param2, Type>,
                lens1: Function1<Type, Param1>,
                lens2: Function1<Type, Param2>,
                codec1: Codec<Param1>,
                codec2: Codec<Param2>
        ): Codec<Type> {
            return Codec2(builder, lens1, lens2, codec1, codec2)
        }

        fun <Type, Param1, Param2, Param3> of(
                builder: Function3<Param1, Param2, Param3, Type>,
                lens1: Function1<Type, Param1>,
                lens2: Function1<Type, Param2>,
                lens3: Function1<Type, Param3>,
                codec1: Codec<Param1>,
                codec2: Codec<Param2>,
                codec3: Codec<Param3>
        ): Codec<Type> {
            return Codec3(builder, lens1, lens2, lens3, codec1, codec2, codec3)
        }

        fun <Type, Param1, Param2, Param3, Param4> of(
                builder: Function4<Param1, Param2, Param3, Param4, Type>,
                lens1: Function1<Type, Param1>,
                lens2: Function1<Type, Param2>,
                lens3: Function1<Type, Param3>,
                lens4: Function1<Type, Param4>,
                codec1: Codec<Param1>,
                codec2: Codec<Param2>,
                codec3: Codec<Param3>,
                codec4: Codec<Param4>
        ): Codec<Type> {
            return Codec4(builder, lens1, lens2, lens3, lens4, codec1, codec2, codec3, codec4)
        }

        fun <Type, Param1, Param2, Param3, Param4, Param5> of(
                builder: Function5<Param1, Param2, Param3, Param4, Param5, Type>,
                lens1: Function1<Type, Param1>,
                lens2: Function1<Type, Param2>,
                lens3: Function1<Type, Param3>,
                lens4: Function1<Type, Param4>,
                lens5: Function1<Type, Param5>,
                codec1: Codec<Param1>,
                codec2: Codec<Param2>,
                codec3: Codec<Param3>,
                codec4: Codec<Param4>,
                codec5: Codec<Param5>
        ): Codec<Type> {
            return Codec5(builder,
                    lens1, lens2, lens3, lens4, lens5,
                    codec1, codec2, codec3, codec4, codec5)
        }

        fun <Type, Param1, Param2, Param3, Param4, Param5, Param6> of(
                builder: Function6<Param1, Param2, Param3, Param4, Param5, Param6, Type>,
                lens1: Function1<Type, Param1>,
                lens2: Function1<Type, Param2>,
                lens3: Function1<Type, Param3>,
                lens4: Function1<Type, Param4>,
                lens5: Function1<Type, Param5>,
                lens6: Function1<Type, Param6>,
                codec1: Codec<Param1>,
                codec2: Codec<Param2>,
                codec3: Codec<Param3>,
                codec4: Codec<Param4>,
                codec5: Codec<Param5>,
                codec6: Codec<Param6>
        ): Codec<Type> {
            return Codec6(builder,
                    lens1, lens2, lens3, lens4, lens5, lens6,
                    codec1, codec2, codec3, codec4, codec5, codec6)
        }
    }
}

internal object BooleanCodec : Codec<Boolean> {
    override fun read(stream: DataInput): Boolean {
        return stream.readBoolean()
    }

    override fun write(stream: DataOutput, value: Boolean) {
        stream.writeBoolean(value)
    }
}

internal object IntCodec : Codec<Int> {
    override fun read(stream: DataInput): Int {
        return stream.readInt()
    }

    override fun write(stream: DataOutput, value: Int) {
        stream.writeInt(value)
    }
}

internal object LongCodec : Codec<Long> {
    override fun read(stream: DataInput): Long {
        return stream.readLong()
    }

    override fun write(stream: DataOutput, value: Long) {
        stream.writeLong(value)
    }
}

internal object StringCodec : Codec<String> {
    override fun read(stream: DataInput): String {
        return stream.readUTF()
    }

    override fun write(stream: DataOutput, value: String) {
        stream.writeUTF(value)
    }
}

internal object ByteArrayCodec : Codec<ByteArray> {
    private val empty = ByteArray(0)

    override fun read(stream: DataInput): ByteArray {
        val size = stream.readInt()
        return if (size > 0) {
            val data = ByteArray(size)
            stream.readFully(data)
            data
        } else {
            empty
        }
    }

    override fun write(stream: DataOutput, value: ByteArray) {
        stream.writeInt(value.size)
        if (value.isNotEmpty()) {
            stream.write(value)
        }
    }
}

internal object ByteBufCodec : Codec<ByteBuf> {

    override fun read(stream: DataInput): ByteBuf {
        val size = stream.readInt()
        if (size == 0) {
            return Unpooled.EMPTY_BUFFER
        }
        val buffer = PooledByteBufAllocator.DEFAULT.buffer(size)
        val max = 64
        val buf = ByteArray(Math.min(size, max))
        var i = 0
        while (i < size) {
            val l = Math.min(max, size - i)
            stream.readFully(buf, 0, l)
            buffer!!.writeBytes(buf, 0, l)
            i += l
        }
        return buffer
    }

    override fun write(stream: DataOutput, value: ByteBuf) {
        val size = value.readableBytes()
        stream.writeInt(size)
        if (size > 0) {
            val max = 64
            val buffer = ByteArray(Math.min(size, max))
            var i = 0
            while (i < size) {
                val l = Math.min(max, size - i)
                value.readBytes(buffer, 0, l)
                stream.write(buffer, 0, l)
                i += l
            }
        }
    }
}

internal class EnumCodec<T : Enum<T>>(private val enumClass: Class<T>) : Codec<T> {

    override fun read(stream: DataInput): T {
        val ord = stream.readUnsignedShort()
        return enumClass.enumConstants[ord]
    }

    override fun write(stream: DataOutput, value: T) {
        stream.writeShort(value.ordinal)
    }
}

internal class OptionCodec<T>(private val codec: Codec<T>) : Codec<Option<T>> {

    override fun read(stream: DataInput): Option<T> {
        return if (stream.readBoolean()) {
            Option.of(codec.read(stream))
        } else {
            Option.none()
        }
    }

    override fun write(stream: DataOutput, value: Option<T>) {
        stream.writeBoolean(value.isDefined)
        value.forEach { i -> codec.write(stream, i) }
    }
}

internal class VectorCodec<T>(private val codec: Codec<T>) : Codec<Vector<T>> {

    override fun read(stream: DataInput): Vector<T> {
        val size = stream.readInt()
        return Vector.fill(size) { codec.read(stream) }
    }

    override fun write(stream: DataOutput, value: Vector<T>) {
        stream.writeInt(value.size())
        value.forEach { i -> codec.write(stream, i) }
    }
}

internal class SetCodec<Type>(private val codec: Codec<Type>) : Codec<Set<Type>> {

    override fun read(stream: DataInput): Set<Type> {
        val size = stream.readInt()
        return HashSet.fill(size) { codec.read(stream) }
    }

    override fun write(stream: DataOutput, value: Set<Type>) {
        stream.writeInt(value.size())
        value.forEach { i -> codec.write(stream, i) }
    }
}

internal class SeqCodec<Type>(private val codec: Codec<Type>) : Codec<Seq<Type>> {

    override fun read(stream: DataInput): Seq<Type> {
        val size = stream.readInt()
        return Vector.fill(size) { codec.read(stream) }
    }

    override fun write(stream: DataOutput, value: Seq<Type>) {
        stream.writeInt(value.size())
        value.forEach { i -> codec.write(stream, i) }
    }
}

internal class Codec1<Type, Param1>(
        private val builder: Function1<Param1, Type>,
        private val lens1: Function1<Type, Param1>,
        private val codec1: Codec<Param1>
) : Codec<Type> {

    override fun read(stream: DataInput): Type {
        val param1 = codec1.read(stream)
        return builder.invoke(param1)
    }

    override fun write(stream: DataOutput, value: Type) {
        codec1.write(stream, lens1.invoke(value))
    }
}

internal class Codec2<Type, Param1, Param2>(
        private val builder: Function2<Param1, Param2, Type>,
        private val lens1: Function1<Type, Param1>,
        private val lens2: Function1<Type, Param2>,
        private val codec1: Codec<Param1>,
        private val codec2: Codec<Param2>
) : Codec<Type> {

    override fun read(stream: DataInput): Type {
        val param1 = codec1.read(stream)
        val param2 = codec2.read(stream)
        return builder.invoke(param1, param2)
    }

    override fun write(stream: DataOutput, value: Type) {
        codec1.write(stream, lens1.invoke(value))
        codec2.write(stream, lens2.invoke(value))
    }
}

internal class Codec3<Type, Param1, Param2, Param3>(
        private val builder: Function3<Param1, Param2, Param3, Type>,
        private val lens1: Function1<Type, Param1>,
        private val lens2: Function1<Type, Param2>,
        private val lens3: Function1<Type, Param3>,
        private val codec1: Codec<Param1>,
        private val codec2: Codec<Param2>,
        private val codec3: Codec<Param3>
) : Codec<Type> {

    override fun read(stream: DataInput): Type {
        val param1 = codec1.read(stream)
        val param2 = codec2.read(stream)
        val param3 = codec3.read(stream)
        return builder.invoke(param1, param2, param3)
    }

    override fun write(stream: DataOutput, value: Type) {
        codec1.write(stream, lens1.invoke(value))
        codec2.write(stream, lens2.invoke(value))
        codec3.write(stream, lens3.invoke(value))
    }
}

internal class Codec4<Type, Param1, Param2, Param3, Param4>(
        private val builder: Function4<Param1, Param2, Param3, Param4, Type>,
        private val lens1: Function1<Type, Param1>,
        private val lens2: Function1<Type, Param2>,
        private val lens3: Function1<Type, Param3>,
        private val lens4: Function1<Type, Param4>,
        private val codec1: Codec<Param1>,
        private val codec2: Codec<Param2>,
        private val codec3: Codec<Param3>,
        private val codec4: Codec<Param4>
) : Codec<Type> {

    override fun read(stream: DataInput): Type {
        val param1 = codec1.read(stream)
        val param2 = codec2.read(stream)
        val param3 = codec3.read(stream)
        val param4 = codec4.read(stream)
        return builder.invoke(param1, param2, param3, param4)
    }

    override fun write(stream: DataOutput, value: Type) {
        codec1.write(stream, lens1.invoke(value))
        codec2.write(stream, lens2.invoke(value))
        codec3.write(stream, lens3.invoke(value))
        codec4.write(stream, lens4.invoke(value))
    }
}

internal class Codec5<Type, Param1, Param2, Param3, Param4, Param5>(
        private val builder: Function5<Param1, Param2, Param3, Param4, Param5, Type>,
        private val lens1: Function1<Type, Param1>,
        private val lens2: Function1<Type, Param2>,
        private val lens3: Function1<Type, Param3>,
        private val lens4: Function1<Type, Param4>,
        private val lens5: Function1<Type, Param5>,
        private val codec1: Codec<Param1>,
        private val codec2: Codec<Param2>,
        private val codec3: Codec<Param3>,
        private val codec4: Codec<Param4>,
        private val codec5: Codec<Param5>
) : Codec<Type> {

    override fun read(stream: DataInput): Type {
        val param1 = codec1.read(stream)
        val param2 = codec2.read(stream)
        val param3 = codec3.read(stream)
        val param4 = codec4.read(stream)
        val param5 = codec5.read(stream)
        return builder.invoke(param1, param2, param3, param4, param5)
    }

    override fun write(stream: DataOutput, value: Type) {
        codec1.write(stream, lens1.invoke(value))
        codec2.write(stream, lens2.invoke(value))
        codec3.write(stream, lens3.invoke(value))
        codec4.write(stream, lens4.invoke(value))
        codec5.write(stream, lens5.invoke(value))
    }
}

internal class Codec6<Type, Param1, Param2, Param3, Param4, Param5, Param6>(
        private val builder: Function6<Param1, Param2, Param3, Param4, Param5, Param6, Type>,
        private val lens1: Function1<Type, Param1>,
        private val lens2: Function1<Type, Param2>,
        private val lens3: Function1<Type, Param3>,
        private val lens4: Function1<Type, Param4>,
        private val lens5: Function1<Type, Param5>,
        private val lens6: Function1<Type, Param6>,
        private val codec1: Codec<Param1>,
        private val codec2: Codec<Param2>,
        private val codec3: Codec<Param3>,
        private val codec4: Codec<Param4>,
        private val codec5: Codec<Param5>,
        private val codec6: Codec<Param6>
) : Codec<Type> {

    override fun read(stream: DataInput): Type {
        val param1 = codec1.read(stream)
        val param2 = codec2.read(stream)
        val param3 = codec3.read(stream)
        val param4 = codec4.read(stream)
        val param5 = codec5.read(stream)
        val param6 = codec6.read(stream)
        return builder.invoke(param1, param2, param3, param4, param5, param6)
    }

    override fun write(stream: DataOutput, value: Type) {
        codec1.write(stream, lens1.invoke(value))
        codec2.write(stream, lens2.invoke(value))
        codec3.write(stream, lens3.invoke(value))
        codec4.write(stream, lens4.invoke(value))
        codec5.write(stream, lens5.invoke(value))
        codec6.write(stream, lens6.invoke(value))
    }
}

internal class StaticCodec<T>(private val value: T) : Codec<T> {

    override fun read(stream: DataInput): T {
        return value
    }

    override fun write(stream: DataOutput, value: T) {}
}

@Suppress("UNCHECKED_CAST")
internal class AnyCodec<T : Message> : Codec<T> {

    override fun read(stream: DataInput): T {
        val id = stream.readInt()
        Preconditions.checkArgument(id >= 0)
        val codec = idToCodecMap.get(id) as Codec<T>
        Preconditions.checkNotNull(codec)
        return codec.read(stream)
    }

    override fun write(stream: DataOutput, value: T) {
        val id = classToIdMap.get(value.javaClass)
        Preconditions.checkArgument(id >= 0, "class not registered %s", value.javaClass)
        val codec = idToCodecMap.get(id) as Codec<T>
        stream.writeInt(id)
        codec.write(stream, value)
    }

    companion object {
        private val idToCodecMap = TIntObjectHashMap<Codec<*>>(100, 0.5f, -1)
        private val classToIdMap = TObjectIntHashMap<Class<*>>(100, 0.5f, -1)

        @Synchronized
        fun <T : Message> register(code: Int, type: Class<T>, codec: Codec<T>) {
            val c = idToCodecMap.putIfAbsent(code, codec)
            Preconditions.checkArgument(c == null, "code already registered: " + code)
            val i = classToIdMap.putIfAbsent(type, code)
            Preconditions.checkArgument(i < 0, "class already registered: " + type)
        }
    }
}