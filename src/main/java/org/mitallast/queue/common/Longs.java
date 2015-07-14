package org.mitallast.queue.common;

import java.util.Random;

public class Longs {
    private final static Random random = new Random();

    /**
     * @link http://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
     */
    public static long nextLong(long max) {
        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (random.nextLong() << 1) >>> 1;
            val = bits % max;
        } while (bits - val + (max - 1) < 0L);
        return val;
    }

    public static byte[] toBytes(long value) {
        byte[] buffer = new byte[8];
        toBytes(buffer, value);
        return buffer;
    }

    public static void toBytes(byte[] buffer, long value) {
        buffer[0] = (byte) (value >>> 56);
        buffer[1] = (byte) (value >>> 48);
        buffer[2] = (byte) (value >>> 40);
        buffer[3] = (byte) (value >>> 32);
        buffer[4] = (byte) (value >>> 24);
        buffer[5] = (byte) (value >>> 16);
        buffer[6] = (byte) (value >>> 8);
        buffer[7] = (byte) value;
    }

    public static long fromBytes(byte[] array) {
        return ((long) array[0] & 0xff) << 56 |
            ((long) array[1] & 0xff) << 48 |
            ((long) array[2] & 0xff) << 40 |
            ((long) array[3] & 0xff) << 32 |
            ((long) array[4] & 0xff) << 24 |
            ((long) array[5] & 0xff) << 16 |
            ((long) array[6] & 0xff) << 8 |
            (long) array[7] & 0xff;
    }
}
