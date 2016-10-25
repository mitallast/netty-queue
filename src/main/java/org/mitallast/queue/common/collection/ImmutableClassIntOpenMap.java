package org.mitallast.queue.common.collection;

public class ImmutableClassIntOpenMap implements ImmutableClassIntMap {

    private final int size;
    private final int emptyValue;
    private final Class[] keys;
    private final int[] values;

    public ImmutableClassIntOpenMap(int size, int emptyValue, Class[] keys, int[] values) {
        this.size = size;
        this.emptyValue = emptyValue;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Class type) {
        return HashFunctions.index(type, keys) >= 0;
    }

    @Override
    public int get(Class type) {
        int index = HashFunctions.index(type, keys);
        if (index >= 0) {
            return values[index];
        }
        return emptyValue;
    }

    @Override
    public void forEach(ClassIntConsumer consumer) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                consumer.accept(keys[i], values[i]);
            }
        }
    }
}
