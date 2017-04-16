package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import javaslang.collection.Seq;
import javaslang.collection.Set;
import javaslang.collection.Vector;
import javaslang.control.Option;

import java.io.OutputStream;

public abstract class StreamOutput extends OutputStream {
    private final StreamableClassRegistry classRegistry;
    private byte[] buffer = new byte[64];

    protected StreamOutput(StreamableClassRegistry classRegistry) {
        this.classRegistry = classRegistry;
    }

    @Override
    public abstract void write(int b);

    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public abstract void write(byte[] b, int off, int len);

    public void writeBoolean(boolean v) {
        write(v ? 1 : 0);
    }

    public void writeUnsignedShort(int v) {
        buffer[0] = (byte) (v >>> 8);
        buffer[1] = (byte) (v);
        write(buffer, 0, 2);
    }

    public void writeShort(short v) {
        buffer[0] = (byte) (v >>> 8);
        buffer[1] = (byte) (v);
        write(buffer, 0, 2);
    }

    public void writeInt(int v) {
        buffer[0] = (byte) (v >>> 24);
        buffer[1] = (byte) (v >>> 16);
        buffer[2] = (byte) (v >>> 8);
        buffer[3] = (byte) (v);
        write(buffer, 0, 4);
    }

    public void writeLong(long v) {
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

    public void writeFloat(float v) {
        writeInt(Float.floatToRawIntBits(v));
    }

    public void writeDouble(double v) {
        writeLong(Double.doubleToRawLongBits(v));
    }

    public final void writeText(String str) {
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
            throw new StreamException("encoded string too long: " + utflen + " bytes");
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

    public final <Type extends Enum<Type>> void writeEnum(Type type) {
        writeUnsignedShort((short) type.ordinal());
    }

    public final void writeByteBuf(ByteBuf buffer) {
        writeByteBuf(buffer, buffer.readableBytes());
    }

    public abstract void writeByteBuf(ByteBuf buffer, int length);

    public final <T extends Streamable> void writeClass(Class<T> streamableClass) {
        classRegistry.writeClass(this, streamableClass);
    }

    public final <T extends Streamable> void writeStreamable(T streamable) {
        streamable.writeTo(this);
    }

    public final <T extends Streamable> void writeTypedVector(Vector<T> list) {
        writeInt(list.size());
        list.forEach(entry -> {
            writeClass(entry.getClass());
            writeStreamable(entry);
        });
    }

    public final <T extends Streamable> void writeVector(Vector<T> list) {
        writeInt(list.size());
        list.forEach(this::writeStreamable);
    }

    public final <T extends Streamable> void writeSet(Set<T> set) {
        writeInt(set.size());
        set.forEach(this::writeStreamable);
    }

    public final <T extends Streamable> void writeSeq(Seq<T> seq) {
        writeInt(seq.size());
        seq.forEach(this::writeStreamable);
    }

    public final <T extends Streamable> void writeOpt(Option<T> option) {
        if (option.isDefined()) {
            writeBoolean(true);
            writeStreamable(option.get());
        } else {
            writeBoolean(false);
        }
    }

    @Override
    public abstract void flush();

    @Override
    public abstract void close();
}
