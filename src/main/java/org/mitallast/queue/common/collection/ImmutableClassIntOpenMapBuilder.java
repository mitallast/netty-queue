package org.mitallast.queue.common.collection;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.Arrays;

public class ImmutableClassIntOpenMapBuilder implements ImmutableClassIntMapBuilder {

    private final static float loadFactor = 0.7f;
    private final TObjectIntMap<Class> map;
    private final int emptyValue;

    public ImmutableClassIntOpenMapBuilder() {
        this(HashFunctions.emptyValue);
    }

    public ImmutableClassIntOpenMapBuilder(int emptyValue) {
        this.map = new TObjectIntHashMap<>();
        this.emptyValue = emptyValue;
    }

    @Override
    public ImmutableClassIntMapBuilder put(Class key, int value) {
        map.put(key, value);
        return this;
    }

    @Override
    public ImmutableClassIntMapBuilder putAll(ImmutableClassIntMap map) {
        map.forEach(this.map::put);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImmutableClassIntMap build() {
        final int size = map.size();
        final int length = HashFunctions.nextPrime(size, loadFactor);
        Class[] keys = new Class[length];
        int[] values = new int[length];
        Arrays.fill(values, emptyValue);

        TObjectIntIterator<Class> iterator = map.iterator();
        for (int i = map.size(); i-- > 0; ) {
            iterator.advance();
            int index = HashFunctions.insert(iterator.key(), keys);
            if (index < 0) {
                throw new RuntimeException("unexpected index");
            }
            values[index] = iterator.value();
        }

        return new ImmutableClassIntOpenMap(size, emptyValue, keys, values);
    }
}
