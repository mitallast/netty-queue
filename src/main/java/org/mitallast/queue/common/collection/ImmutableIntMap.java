package org.mitallast.queue.common.collection;

public interface ImmutableIntMap<V> {

    public final static ImmutableIntMap EMPTY = new ImmutableIntMap<Object>() {
        @Override
        public int emptyKey() {
            return 0;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean containsKey(int key) {
            return false;
        }

        @Override
        public Object get(int key) {
            return null;
        }

        @Override
        public void forEach(IntMapConsumer<Object> consumer) {

        }
    };

    public static <V> ImmutableIntMapBuilder<V> builder() {
        return new ImmutableIntOpenMapBuilder<V>();
    }

    public static <V> ImmutableIntMapBuilder<V> builder(int emptyKey) {
        return new ImmutableIntOpenMapBuilder<V>(emptyKey);
    }

    @SuppressWarnings("unchecked")
    public static <V> ImmutableIntMap<V> empty() {
        return EMPTY;
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
