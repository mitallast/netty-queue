package org.mitallast.queue.common.builder;

public interface Entry<B extends EntryBuilder<B, E>, E extends Entry<B, E>> {

    B toBuilder();
}
