package org.mitallast.queue.common.collection;

public class ImmutableIntOpenMap<V> implements ImmutableIntMap<V> {

    private final int size;
    private final int emptyKey;
    private final int[] keys;
    private final V[] values;

    public ImmutableIntOpenMap(int size, int emptyKey, int[] keys, V[] values) {
        this.size = size;
        this.emptyKey = emptyKey;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public int emptyKey() {
        return emptyKey;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size > 0;
    }

    @Override
    public boolean containsKey(int key) {
        return HashFunctions.index(key, emptyKey, keys) >= 0;
    }

    @Override
    public V get(int key) {
        int index = HashFunctions.index(key, keys);
        if (index >= 0) {
            return values[index];
        }
        return null;
    }

    @Override
    public void forEach(IntMapConsumer<V> consumer) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != emptyKey) {
                consumer.accept(keys[i], values[i]);
            }
        }
    }
}
