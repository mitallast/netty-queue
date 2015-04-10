package org.mitallast.queue.common.concurrent.futures;

import org.mitallast.queue.common.concurrent.Listener;

public interface ListenerSmartFuture<Type> extends SmartFuture<Type>, Listener<Type> {
    @Override
    default void onResponse(Type result) {
        invoke(result);
    }

    @Override
    default void onFailure(Throwable e) {
        invokeException(e);
    }
}
