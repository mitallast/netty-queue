package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;

public interface AsciiString {

    char charAt(int index);

    byte byteAt(int index);

    boolean isEmpty();

    int length();

    AsciiString substring(int beginIndex);

    AsciiString substring(int beginIndex, int len);

    void write(ByteBuf out);

    default long longValue() {
        long v = 0;
        int l = length();
        for (int i = 0; i < l; i++) {
            v = v * 10;
            char c = charAt(i);
            if (c >= '0' && c <= '9') {
                v += c - '0';
            } else {
                throw new IllegalArgumentException();
            }
        }
        return v;
    }

    default int intValue() {
        int v = 0;
        int l = length();
        for (int i = 0; i < l; i++) {
            v = v * 10;
            char c = charAt(i);
            if (c >= '0' && c <= '9') {
                v += c - '0';
            } else {
                throw new IllegalArgumentException("char: [" + c + "]");
            }
        }
        return v;
    }

    default String string() {
        StringBuilder builder = new StringBuilder(length());
        for (int i = 0; i < length(); i++) {
            builder.append(charAt(i));
        }
        return builder.toString();
    }

    byte[] array();

    default void release() {
    }

    final class ByteArrayAsciiString implements AsciiString {
        private final byte[] value;

        private ByteArrayAsciiString(byte[] value) {
            this.value = value;
        }

        @Override
        public char charAt(int index) {
            return b2c(byteAt(index));
        }

        @Override
        public byte byteAt(int index) {
            if ((index < 0) || (index >= value.length)) {
                throw new StringIndexOutOfBoundsException(index);
            }
            return value[index];
        }

        @Override
        public boolean isEmpty() {
            return value.length == 0;
        }

        @Override
        public int length() {
            return value.length;
        }

        @Override
        public AsciiString substring(int beginIndex) {
            int subLen = value.length - beginIndex;
            return (beginIndex == 0) ? this : AsciiString.of(value, beginIndex, subLen);
        }

        @Override
        public AsciiString substring(int beginIndex, int len) {
            return (beginIndex == 0 && len == value.length) ? this : AsciiString.of(value, beginIndex, len);
        }

        @Override
        public void write(ByteBuf out) {
            out.writeBytes(value);
        }

        @Override
        public String toString() {
            return string();
        }

        @Override
        public byte[] array() {
            return value;
        }
    }

    final class ByteBufAsciiString implements AsciiString {
        private final ByteBuf buf;
        private byte[] bytes;

        private ByteBufAsciiString(ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public char charAt(int index) {
            return b2c(byteAt(index));
        }

        @Override
        public byte byteAt(int index) {
            int offset = buf.readerIndex();
            return buf.getByte(offset + index);
        }

        @Override
        public boolean isEmpty() {
            return buf.isReadable();
        }

        @Override
        public int length() {
            return buf.readableBytes();
        }

        @Override
        public AsciiString substring(int beginIndex) {
            int subLen = buf.readableBytes() - beginIndex;
            return (beginIndex == 0) ? this : AsciiString.of(buf, beginIndex, subLen);
        }

        @Override
        public AsciiString substring(int beginIndex, int len) {
            return (beginIndex == 0 && len == buf.readableBytes()) ? this : AsciiString.of(buf, beginIndex, len);
        }

        @Override
        public void write(ByteBuf out) {
            buf.markReaderIndex();
            buf.readBytes(out);
            buf.resetReaderIndex();
        }

        @Override
        public String toString() {
            return string();
        }

        @Override
        public byte[] array() {
            int len = buf.readableBytes();
            if (buf.hasArray()) {
                int offset = buf.arrayOffset();
                bytes = Arrays.copyOfRange(buf.array(), offset, offset + len);
            } else {
                bytes = new byte[len];
                buf.markReaderIndex();
                buf.readBytes(bytes);
                buf.resetReaderIndex();
            }
            return bytes;
        }

        @Override
        public void release() {
            buf.release();
        }
    }

    char MAX_CHAR_VALUE = 255;

    static AsciiString of(String string) {
        byte[] value = new byte[string.length()];
        for (int i = 0, l = string.length(); i < l; i++) {
            value[i] = (byte) string.charAt(i);
        }
        return new ByteArrayAsciiString(value);
    }

    static AsciiString of(byte[] value) {
        return new ByteArrayAsciiString(value);
    }

    static AsciiString of(byte[] value, int offset, int len) {
        if (offset < 0 || offset > value.length) {
            throw new StringIndexOutOfBoundsException(offset);
        }
        if (offset + len > value.length) {
            throw new StringIndexOutOfBoundsException(len);
        }
        byte[] bytes = new byte[len];
        System.arraycopy(value, offset, bytes, 0, len);
        return new ByteArrayAsciiString(bytes);
    }

    static AsciiString of(ByteBuf value) {
        return new ByteBufAsciiString(value);
    }

    static AsciiString of(ByteBuf value, int offset, int len) {
        if (offset < 0 || offset > value.readableBytes()) {
            throw new StringIndexOutOfBoundsException(offset);
        }
        if (offset + len > value.readableBytes()) {
            throw new StringIndexOutOfBoundsException(len);
        }
        int read = value.readerIndex();
        ByteBuf slice = value.slice(read + offset, len);
        return new ByteBufAsciiString(slice);
    }

    static boolean equalsIgnoreCase(byte a, byte b) {
        return a == b || toLowerCase(a) == toLowerCase(b);
    }

    static boolean equalsIgnoreCase(char a, char b) {
        return a == b || toLowerCase(a) == toLowerCase(b);
    }

    static byte toLowerCase(byte b) {
        return isUpperCase(b) ? (byte) (b + 32) : b;
    }

    static char toLowerCase(char c) {
        return isUpperCase(c) ? (char) (c + 32) : c;
    }

    static byte toUpperCase(byte b) {
        return isLowerCase(b) ? (byte) (b - 32) : b;
    }

    static boolean isLowerCase(byte value) {
        return value >= 'a' && value <= 'z';
    }

    static boolean isUpperCase(byte value) {
        return value >= 'A' && value <= 'Z';
    }

    static boolean isUpperCase(char value) {
        return value >= 'A' && value <= 'Z';
    }

    static byte c2b(char c) {
        return (byte) ((c > MAX_CHAR_VALUE) ? '?' : c);
    }

    static char b2c(byte b) {
        return (char) (b & 0xFF);
    }

    static char b2cL(byte b) {
        return toLowerCase(b2c(b));
    }
}
