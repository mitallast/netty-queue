package org.mitallast.queue.action;

import org.mitallast.queue.common.builder.Entry;

public interface ActionResponse<E extends ActionResponse> extends Entry<E> {

    default boolean hasError() {
        return error() != null;
    }

    default ActionResponseError error() {
        return null;
    }
}
