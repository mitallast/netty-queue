package org.mitallast.queue.common.collection;

public interface ImmutableLongMapBuilder<V> {

    ImmutableLongMapBuilder<V> put(long key, V value);

    ImmutableLongMapBuilder<V> putAll(ImmutableLongMap<V> map);

    ImmutableLongMap<V> build();
}
