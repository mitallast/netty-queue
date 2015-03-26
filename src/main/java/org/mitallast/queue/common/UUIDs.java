package org.mitallast.queue.common;

import com.eaio.util.lang.Hex;
import com.eaio.uuid.UUIDGen;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.AsciiString;

import java.util.UUID;

public class UUIDs {

    final static char[] digits = {
        '0', '1', '2', '3', '4', '5',
        '6', '7', '8', '9', 'a', 'b',
        'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't',
        'u', 'v', 'w', 'x', 'y', 'z'
    };

    public static UUID generateRandom() {
        return new UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
    }

    public static CharSequence toCharSequence(UUID uuid) {
        return toCharSequence(new StringBuilder(36), uuid);
    }

    public static CharSequence toCharSequence(StringBuilder out, UUID uuid) {
        Hex.append(out, (int) (uuid.getMostSignificantBits() >> 32));
        out.append('-');
        Hex.append(out, (short) (uuid.getMostSignificantBits() >> 16));
        out.append('-');
        Hex.append(out, (short) uuid.getMostSignificantBits());
        out.append('-');
        Hex.append(out, (short) (uuid.getLeastSignificantBits() >> 48));
        out.append('-');
        Hex.append(out, uuid.getLeastSignificantBits(), 12);
        return out;
    }

    public static UUID fromString(CharSequence sequence) {
        AsciiString name = AsciiString.of(sequence);
        AsciiString[] components = name.split('-');
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: " + name);


        long mostSigBits = components[0].parseLong(16);
        mostSigBits <<= 16;
        mostSigBits |= components[1].parseLong(16);
        mostSigBits <<= 16;
        mostSigBits |= components[2].parseLong(16);

        long leastSigBits = components[3].parseLong(16);
        leastSigBits <<= 48;
        leastSigBits |= components[4].parseLong(16);

        return new UUID(mostSigBits, leastSigBits);
    }

    public static ByteBuf toByteBuf(UUID uuid) {
        ByteBuf buffer = Unpooled.buffer(36);
        writeTo(uuid, buffer);
        return buffer;
    }

    public static ByteBuf writeTo(UUID uuid, ByteBuf buffer) {
        digits(uuid.getMostSignificantBits() >> 32, 8, buffer);
        buffer.writeByte((byte) '-');
        digits(uuid.getMostSignificantBits() >> 16, 4, buffer);
        buffer.writeByte((byte) '-');
        digits(uuid.getMostSignificantBits(), 4, buffer);
        buffer.writeByte((byte) '-');
        digits(uuid.getLeastSignificantBits() >> 48, 4, buffer);
        buffer.writeByte((byte) '-');
        digits(uuid.getLeastSignificantBits(), 12, buffer);
        return buffer;
    }

    /**
     * Returns val represented by the specified number of hex digits.
     */
    private static void digits(long val, int digits, ByteBuf buffer) {
        long hi = 1L << (digits * 4);
        toHexString(hi | (val & (hi - 1)), buffer);
    }

    /**
     * Returns a string representation of the {@code long}
     * argument as an unsigned integer in base&nbsp;16.
     */
    private static void toHexString(long i, ByteBuf buffer) {
        toUnsignedString(i, 4, buffer);
    }

    /**
     * Format a long (treated as unsigned) into a String.
     *
     * @param val   the value to format
     * @param shift the log2 of the base to format in (4 for hex, 3 for octal, 1 for binary)
     */
    private static void toUnsignedString(long val, int shift, ByteBuf buffer) {
        // assert shift > 0 && shift <=5 : "Illegal shift value";
        int mag = Long.SIZE - Long.numberOfLeadingZeros(val);
        int chars = Math.max(((mag + (shift - 1)) / shift), 1) - 1;

        formatUnsignedLong(val, shift, buffer, chars);
    }

    /**
     * Format a long (treated as unsigned) into a character buffer.
     *
     * @param val    the unsigned long to format
     * @param shift  the log2 of the base to format in (4 for hex, 3 for octal, 1 for binary)
     * @param buffer the character buffer to write to
     * @param len    the number of characters to write
     * @return the lowest character location used
     */
    private static int formatUnsignedLong(long val, int shift, ByteBuf buffer, int len) {
        int charPos = len;
        int radix = 1 << shift;
        int mask = radix - 1;
        int bufferPos = buffer.writerIndex();
        buffer.ensureWritable(len);
        do {
            buffer.setByte(bufferPos + --charPos, (byte) digits[((int) val) & mask]);
            val >>>= shift;
        } while (val != 0 && charPos > 0);

        buffer.writerIndex(bufferPos + len);
        return charPos;
    }
}
