package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.settings.Settings;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public interface StreamOutput extends DataOutput, Closeable {

    @Override
    void write(int b) throws IOException;

    @Override
    void write(byte[] b) throws IOException;

    @Override
    void write(byte[] b, int off, int len) throws IOException;

    @Override
    void writeBoolean(boolean v) throws IOException;

    @Override
    void writeByte(int v) throws IOException;

    @Override
    void writeShort(int v) throws IOException;

    @Override
    void writeChar(int v) throws IOException;

    @Override
    void writeInt(int v) throws IOException;

    @Override
    void writeLong(long v) throws IOException;

    @Override
    void writeFloat(float v) throws IOException;

    @Override
    void writeDouble(double v) throws IOException;

    void writeText(String text) throws IOException;

    void writeTextOrNull(String text) throws IOException;

    void writeUUID(UUID uuid) throws IOException;

    void writeUUIDOrNull(UUID uuid) throws IOException;

    <Type extends Enum<Type>> void writeEnum(Type type) throws IOException;

    <Type extends Enum<Type>> void writeEnumOrNull(Type type) throws IOException;

    void writeByteBuf(ByteBuf buffer) throws IOException;

    void writeByteBuf(ByteBuf buffer, int length) throws IOException;

    void writeByteBufOrNull(ByteBuf buffer) throws IOException;

    void writeByteBufOrNull(ByteBuf buffer, int length) throws IOException;

    void writeSettings(Settings settings) throws IOException;

    <T extends Streamable> void writeStreamable(T streamable) throws IOException;

    <T extends Streamable> void writeStreamableOrNull(T streamable) throws IOException;
}
