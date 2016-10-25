package org.mitallast.queue.common.collection;

public class ImmutableLongOpenMap<V> implements ImmutableLongMap<V> {

    private final int size;
    private final long emptyKey;
    private final long[] keys;
    private final V[] values;

    public ImmutableLongOpenMap(int size, long emptyKey, long[] keys, V[] values) {
        this.size = size;
        this.emptyKey = emptyKey;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public long emptyKey() {
        return emptyKey;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(long key) {
        return HashFunctions.index(key, emptyKey, keys) >= 0;
    }

    @Override
    public V get(long key) {
        int index = HashFunctions.index(key, keys);
        if (index >= 0) {
            return values[index];
        }
        return null;
    }

    @Override
    public void forEach(LongMapConsumer<V> consumer) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != emptyKey) {
                consumer.accept(keys[i], values[i]);
            }
        }
    }
}
