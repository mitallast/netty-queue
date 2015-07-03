package org.mitallast.queue.common.builder;

import org.mitallast.queue.common.stream.Streamable;

public interface EntryBuilder<E> extends Streamable {

    E build();
}
