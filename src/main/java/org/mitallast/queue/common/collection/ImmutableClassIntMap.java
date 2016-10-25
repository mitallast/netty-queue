package org.mitallast.queue.common.collection;

public interface ImmutableClassIntMap {

    ImmutableClassIntMap EMPTY = new ImmutableClassIntMap() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean containsKey(Class type) {
            return false;
        }

        @Override
        public int get(Class type) {
            return HashFunctions.emptyValue;
        }

        @Override
        public void forEach(ClassIntConsumer consumer) {

        }
    };

    static ImmutableClassIntMapBuilder builder() {
        return new ImmutableClassIntOpenMapBuilder();
    }

    static ImmutableClassIntMapBuilder builder(int emptyValue) {
        return new ImmutableClassIntOpenMapBuilder(emptyValue);
    }

    @SuppressWarnings("unchecked")
    static ImmutableClassIntMap empty() {
        return EMPTY;
    }

    int size();

    boolean isEmpty();

    boolean containsKey(Class type);

    int get(Class type);

    void forEach(ClassIntConsumer consumer);

    interface ClassIntConsumer {
        void accept(Class type, int value);
    }
}
