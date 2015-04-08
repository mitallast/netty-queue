package org.mitallast.queue.common.collection;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.Arrays;

public class ImmutableLongOpenMapBuilder<V> implements ImmutableLongMapBuilder<V> {

    private final static float loadFactor = 0.7f;
    private final TLongObjectMap<V> map;
    private final long emptyKey;

    public ImmutableLongOpenMapBuilder() {
        this(HashFunctions.emptyKey);
    }

    public ImmutableLongOpenMapBuilder(long emptyKey) {
        map = new TLongObjectHashMap<>();
        this.emptyKey = emptyKey;
    }

    @Override
    public ImmutableLongMapBuilder<V> put(long key, V value) {
        map.put(key, value);
        return this;
    }

    @Override
    public ImmutableLongMapBuilder<V> putAll(ImmutableLongMap<V> map) {
        map.forEach(this.map::put);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImmutableLongMap<V> build() {
        final int size = map.size();
        final int length = HashFunctions.nextPrime(size, loadFactor);
        long[] keys = new long[length];
        Arrays.fill(keys, emptyKey);
        V[] values = (V[]) new Object[length];

        TLongObjectIterator<V> iterator = map.iterator();
        for (int i = map.size(); i-- > 0; ) {
            iterator.advance();
            int index = HashFunctions.insert(iterator.key(), emptyKey, keys);
            if (index < 0) {
                throw new RuntimeException("unexpected index");
            }
            values[index] = iterator.value();
        }

        return new ImmutableLongOpenMap(size, emptyKey, keys, values);
    }
}
