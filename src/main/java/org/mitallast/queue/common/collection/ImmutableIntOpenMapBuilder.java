package org.mitallast.queue.common.collection;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Arrays;

public class ImmutableIntOpenMapBuilder<V> implements ImmutableIntMapBuilder<V> {

    private final static float loadFactor = 0.5f;
    private final TIntObjectMap<V> map;
    private final int emptyKey;

    public ImmutableIntOpenMapBuilder() {
        this(HashFunctions.emptyKey);
    }

    public ImmutableIntOpenMapBuilder(int emptyKey) {
        map = new TIntObjectHashMap<>();
        this.emptyKey = emptyKey;
    }

    @Override
    public ImmutableIntMapBuilder<V> put(int key, V value) {
        map.put(key, value);
        return this;
    }

    @Override
    public ImmutableIntMapBuilder<V> putAll(ImmutableIntMap<V> map) {
        map.forEach(this.map::put);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImmutableIntMap<V> build() {
        final int size = map.size();
        final int length = HashFunctions.fastCeil(size / loadFactor);
        int[] keys = new int[length];
        Arrays.fill(keys, emptyKey);
        V[] values = (V[]) new Object[length];

        TIntObjectIterator<V> iterator = map.iterator();
        for (int i = map.size(); i-- > 0; ) {
            iterator.advance();
            int index = HashFunctions.insert(iterator.key(), emptyKey, keys);
            if (index < 0) {
                throw new RuntimeException("unexpected index");
            }
            values[index] = iterator.value();
        }

        return new ImmutableIntOpenMap(size, emptyKey, keys, values);
    }
}
