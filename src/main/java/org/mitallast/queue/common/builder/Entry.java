package org.mitallast.queue.common.builder;

public interface Entry<E extends Entry> {

    EntryBuilder<E> toBuilder();
}
