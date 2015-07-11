package org.mitallast.queue.action;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.stream.StreamableError;

public interface ActionResponse<E extends ActionResponse> extends Entry<E> {

    default boolean hasError() {
        return error() != null;
    }

    default StreamableError error() {
        return null;
    }
}
