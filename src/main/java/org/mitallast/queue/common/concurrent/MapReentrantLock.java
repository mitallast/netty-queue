package org.mitallast.queue.common.concurrent;

import java.util.concurrent.locks.ReentrantLock;

public class MapReentrantLock {
    private final ReentrantLock locks[];

    public MapReentrantLock(final int size) {
        this.locks = new ReentrantLock[size];
        for (int i = 0; i < size; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    private static int hash(long value) {
        return (int) (value ^ (value >>> 32));
    }

    private static int offset(int hash, int size) {
        return hash % size;
    }

    public ReentrantLock get(long key) {
        int offset = offset(hash(key), locks.length);
        return locks[offset];
    }
}
