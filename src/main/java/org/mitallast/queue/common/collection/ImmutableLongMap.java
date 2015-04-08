package org.mitallast.queue.common.collection;

public interface ImmutableLongMap<V> {

    public final static ImmutableLongMap EMPTY = new ImmutableLongMap<Object>() {
        @Override
        public long emptyKey() {
            return 0;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean containsKey(long key) {
            return false;
        }

        @Override
        public Object get(long key) {
            return null;
        }

        @Override
        public void forEach(LongMapConsumer<Object> consumer) {

        }
    };

    public static <V> ImmutableLongMapBuilder<V> builder() {
        return new ImmutableLongOpenMapBuilder<V>();
    }

    public static <V> ImmutableLongMapBuilder<V> builder(long emptyKey) {
        return new ImmutableLongOpenMapBuilder<V>(emptyKey);
    }

    @SuppressWarnings("unchecked")
    public static <V> ImmutableLongMap<V> empty() {
        return EMPTY;
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
