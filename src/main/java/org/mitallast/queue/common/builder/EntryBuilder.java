package org.mitallast.queue.common.builder;

import org.mitallast.queue.common.stream.Streamable;

public interface EntryBuilder<B extends EntryBuilder<B, E>, E extends Entry> extends Streamable {

    B from(E entry);

    E build();
}
