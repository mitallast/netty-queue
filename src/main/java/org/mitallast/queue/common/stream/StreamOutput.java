package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.util.List;
import java.util.Set;

public abstract class StreamOutput extends OutputStream {
    private final StreamableClassRegistry classRegistry;
    private byte[] buffer = new byte[64];

    protected StreamOutput(StreamableClassRegistry classRegistry) {
        this.classRegistry = classRegistry;
    }

    @Override
    public abstract void write(int b) throws IOException;

    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public void writeUnsignedShort(int v) throws IOException {
        buffer[0] = (byte) (v >>> 8);
        buffer[1] = (byte) (v);
        write(buffer, 0, 2);
    }

    public void writeShort(short v) throws IOException {
        buffer[0] = (byte) (v >>> 8);
        buffer[1] = (byte) (v);
        write(buffer, 0, 2);
    }

    public void writeInt(int v) throws IOException {
        buffer[0] = (byte) (v >>> 24);
        buffer[1] = (byte) (v >>> 16);
        buffer[2] = (byte) (v >>> 8);
        buffer[3] = (byte) (v);
        write(buffer, 0, 4);
    }

    public void writeLong(long v) throws IOException {
        buffer[0] = (byte) (v >>> 56);
        buffer[1] = (byte) (v >>> 48);
        buffer[2] = (byte) (v >>> 40);
        buffer[3] = (byte) (v >>> 32);
        buffer[4] = (byte) (v >>> 24);
        buffer[5] = (byte) (v >>> 16);
        buffer[6] = (byte) (v >>> 8);
        buffer[7] = (byte) (v);
        write(buffer, 0, 8);
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToRawIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToRawLongBits(v));
    }

    public final void writeText(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");
        }

        if (buffer == null || buffer.length < utflen + 2) {
            buffer = new byte[utflen + 2];
        }

        buffer[count++] = (byte) ((utflen >>> 8) & 0xFF);
        buffer[count++] = (byte) ((utflen) & 0xFF);

        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) break;
            buffer[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffer[count++] = (byte) c;

            } else if (c > 0x07FF) {
                buffer[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                buffer[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                buffer[count++] = (byte) (0x80 | ((c) & 0x3F));
            } else {
                buffer[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                buffer[count++] = (byte) (0x80 | ((c) & 0x3F));
            }
        }
        write(buffer, 0, utflen + 2);
    }

    public final <Type extends Enum<Type>> void writeEnum(Type type) throws IOException {
        writeUnsignedShort((short) type.ordinal());
    }

    public final void writeByteBuf(ByteBuf buffer) throws IOException {
        writeByteBuf(buffer, buffer.readableBytes());
    }

    public abstract void writeByteBuf(ByteBuf buffer, int length) throws IOException;

    public final <T extends Streamable> void writeClass(Class<T> streamableClass) throws IOException {
        classRegistry.writeClass(this, streamableClass);
    }

    public final <T extends Streamable> void writeStreamable(T streamable) throws IOException {
        streamable.writeTo(this);
    }

    public final <T extends Streamable> void writeStreamableOrNull(T streamable) throws IOException {
        if (streamable != null) {
            writeBoolean(true);
            streamable.writeTo(this);
        } else {
            writeBoolean(false);
        }
    }

    public final <T extends Streamable> void writeStreamableList(List<T> streamable) throws IOException {
        int size = streamable.size();
        writeInt(size);
        if (size > 0) {
            for (T t : streamable) {
                t.writeTo(this);
            }
        }
    }

    public final <T extends Streamable> void writeStreamableSet(Set<T> streamable) throws IOException {
        int size = streamable.size();
        writeInt(size);
        if (size > 0) {
            for (T t : streamable) {
                t.writeTo(this);
            }
        }
    }
}
