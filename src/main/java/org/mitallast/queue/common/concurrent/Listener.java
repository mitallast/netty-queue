package org.mitallast.queue.common.concurrent;

public interface Listener<T> {

    public void onResponse(T queueMessage);

    void onFailure(Throwable e);
}