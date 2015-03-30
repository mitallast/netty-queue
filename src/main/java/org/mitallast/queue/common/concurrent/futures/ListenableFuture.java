package org.mitallast.queue.common.concurrent.futures;

import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface ListenableFuture<T> extends Future<T> {

    void on(Consumer<FutureResult<T, Throwable>> listener);
}
