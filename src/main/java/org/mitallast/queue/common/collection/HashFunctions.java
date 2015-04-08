package org.mitallast.queue.common.collection;

import gnu.trove.impl.PrimeFinder;

import static gnu.trove.impl.HashFunctions.fastCeil;

public class HashFunctions {

    public static int emptyKey = -1;

    public static int nextPrime(int size, float loadFactor) {
        int ceil = fastCeil(size / loadFactor);
        return PrimeFinder.nextPrime(ceil);
    }

    public static int insert(int key, int[] keys) {
        return insert(key, emptyKey, keys);
    }

    public static int insert(int key, int emptyKey, int[] keys) {
        final int hash = key & 0x7fffffff;
        int size = keys.length;
        int index = hash % size;

        if (keys[index] == emptyKey) {
            keys[index] = key;
            return index;
        } else if (keys[index] == key) {
            return index;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (keys[index] == emptyKey) {
                keys[index] = key;
                return index;
            } else if (keys[index] == key) {
                return index;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }

    public static int index(int key, int[] keys) {
        return index(key, emptyKey, keys);
    }

    public static int index(int key, int emptyKey, int[] keys) {
        final int hash = key & 0x7fffffff;
        int size = keys.length;
        int index = hash % size;

        if (keys[index] == emptyKey) {
            return -1;
        } else if (keys[index] == key) {
            return index;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (keys[index] == emptyKey) {
                return -1;
            } else if (keys[index] == key) {
                return index;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }

    public static int insert(long key, long[] keys) {
        return insert(key, emptyKey, keys);
    }

    public static int insert(long key, long emptyKey, long[] keys) {
        final int hash = (int) (key ^ (key >>> 32)) & 0x7fffffff;
        int size = keys.length;
        int index = hash % size;

        if (keys[index] == emptyKey) {
            keys[index] = key;
            return index;
        } else if (keys[index] == key) {
            return index;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (keys[index] == emptyKey) {
                keys[index] = key;
                return index;
            } else if (keys[index] == key) {
                return index;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }

    public static int index(long key, long[] keys) {
        return index(key, emptyKey, keys);
    }

    public static int index(long key, long emptyKey, long[] keys) {
        final int hash = (int) (key ^ (key >>> 32)) & 0x7fffffff;
        int size = keys.length;
        int index = hash % size;

        if (keys[index] == emptyKey) {
            return -1;
        } else if (keys[index] == key) {
            return index;
        }

        final int loopIndex = index;
        final int probe = 1 + (hash % (size - 2));
        do {
            index = index - probe;
            if (index < 0) {
                index += size;
            }

            if (keys[index] == emptyKey) {
                return -1;
            } else if (keys[index] == key) {
                return index;
            }
            // Detect loop
        } while (index != loopIndex);
        return -1;
    }
}
