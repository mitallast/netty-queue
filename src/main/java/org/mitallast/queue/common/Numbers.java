package org.mitallast.queue.common;

import java.util.Random;

public class Numbers {
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
}
