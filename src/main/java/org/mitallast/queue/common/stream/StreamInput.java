package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.settings.Settings;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

public interface StreamInput extends DataInput, Closeable {

    @Override
    void readFully(byte[] b) throws IOException;

    @Override
    void readFully(byte[] b, int off, int len) throws IOException;

    @Override
    int skipBytes(int n) throws IOException;

    @Override
    boolean readBoolean() throws IOException;

    @Override
    byte readByte() throws IOException;

    @Override
    int readUnsignedByte() throws IOException;

    @Override
    short readShort() throws IOException;

    @Override
    int readUnsignedShort() throws IOException;

    @Override
    char readChar() throws IOException;

    @Override
    int readInt() throws IOException;

    @Override
    long readLong() throws IOException;

    @Override
    float readFloat() throws IOException;

    @Override
    double readDouble() throws IOException;

    String readText() throws IOException;

    String readTextOrNull() throws IOException;

    UUID readUUID() throws IOException;

    UUID readUUIDOrNull() throws IOException;

    <Type extends Enum<Type>> Type readEnum(Class<Type> enumClass) throws IOException;

    <Type extends Enum<Type>> Type readEnumOrNull(Class<Type> enumClass) throws IOException;

    ByteBuf readByteBuf() throws IOException;

    ByteBuf readByteBufOrNull() throws IOException;

    Settings readSettings() throws IOException;

    <T extends Streamable> T readStreamable(Supplier<T> factory) throws IOException;

    <T extends Streamable> T readStreamableOrNull(Supplier<T> factory) throws IOException;
}
