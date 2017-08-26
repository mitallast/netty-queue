package org.mitallast.queue.common.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

final class EnumCodec<Type extends Enum<Type>> implements Codec<Type> {
    private final Class<Type> enumClass;

    EnumCodec(Class<Type> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public Type read(DataInput stream) {
        try {
            int ord = stream.readUnsignedShort();
            return enumClass.getEnumConstants()[ord];
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, Type value) {
        try {
            stream.writeShort(value.ordinal());
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
