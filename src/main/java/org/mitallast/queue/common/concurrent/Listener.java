package org.mitallast.queue.common.concurrent;

public interface Listener<T> {

    public void onResponse(T result);

    void onFailure(Throwable e);
}