package org.mitallast.queue.common.collection;

public interface ImmutableIntMapBuilder<V> {

    ImmutableIntMapBuilder<V> put(int key, V value);

    ImmutableIntMapBuilder<V> putAll(ImmutableIntMap<V> map);

    ImmutableIntMap<V> build();
}
