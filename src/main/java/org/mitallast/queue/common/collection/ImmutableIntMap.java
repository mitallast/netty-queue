package org.mitallast.queue.common.collection;

public interface ImmutableIntMap<V> {

    public static <V> ImmutableIntMapBuilder<V> builder() {
        return new ImmutableIntOpenMapBuilder<V>();
    }

    public static <V> ImmutableIntMapBuilder<V> builder(int emptyKey) {
        return new ImmutableIntOpenMapBuilder<V>(emptyKey);
    }

    int emptyKey();

    int size();

    boolean isEmpty();

    boolean containsKey(int key);

    V get(int key);

    void forEach(IntMapConsumer<V> consumer);

    public static interface IntMapConsumer<V> {
        void accept(int key, V value);
    }
}
