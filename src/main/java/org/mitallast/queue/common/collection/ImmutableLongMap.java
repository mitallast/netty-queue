package org.mitallast.queue.common.collection;

import org.mitallast.queue.crdt.Crdt;

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
            return true;
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

    static <V> ImmutableLongMapBuilder<V> builder() {
        return new ImmutableLongOpenMapBuilder<V>();
    }

    static <V> ImmutableLongMapBuilder<V> builder(long emptyKey) {
        return new ImmutableLongOpenMapBuilder<V>(emptyKey);
    }

    @SuppressWarnings("unchecked")
    static <V> ImmutableLongMap<V> empty() {
        return EMPTY;
    }

    long emptyKey();

    int size();

    boolean isEmpty();

    boolean containsKey(long key);

    V get(long key);

    void forEach(LongMapConsumer<V> consumer);

    default ImmutableLongMap<V> remove(long removeKey) {
        ImmutableLongMapBuilder<V> builder = builder(emptyKey());
        forEach((key, value) -> {
            if (key != removeKey) {
                builder.put(key, value);
            }
        });
        return builder.build();
    }

    interface LongMapConsumer<V> {
        void accept(long key, V value);
    }
}
