package org.mitallast.queue.common.collection;

public interface ImmutableClassIntMapBuilder {

    ImmutableClassIntMapBuilder put(Class key, int value);

    ImmutableClassIntMapBuilder putAll(ImmutableClassIntMap map);

    ImmutableClassIntMap build();
}
