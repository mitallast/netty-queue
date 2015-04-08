package org.mitallast.queue.common.collection;

public interface ImmutableLongMap<V> {

    public static <V> ImmutableLongMapBuilder<V> builder() {
        return new ImmutableLongOpenMapBuilder<V>();
    }

    public static <V> ImmutableLongMapBuilder<V> builder(long emptyKey) {
        return new ImmutableLongOpenMapBuilder<V>(emptyKey);
    }

    long emptyKey();

    int size();

    boolean isEmpty();

    boolean containsKey(long key);

    V get(long key);

    void forEach(LongMapConsumer<V> consumer);

    public static interface LongMapConsumer<V> {
        void accept(long key, V value);
    }
}
