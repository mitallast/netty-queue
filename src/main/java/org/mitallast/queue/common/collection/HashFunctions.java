package org.mitallast.queue.common.collection;

public class HashFunctions {

    public static int emptyKey = -1;

    public static int fastCeil(float v) {
        return gnu.trove.impl.HashFunctions.fastCeil(v);
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
}
