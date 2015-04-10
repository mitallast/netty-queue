package org.mitallast.queue.common.concurrent.futures;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FutureResult<Type, Error> {

    @Nullable
    Type getOrNull();

    default
    @Nonnull
    Type getOrDefault(@Nonnull Type defValue) {
        Type value = this.getOrNull();
        return value == null ? defValue : value;
    }

    default boolean isValuableDone() {
        return !this.isError() && !this.isCancelled();
    }

    default boolean isExceptional() {
        return this.isError() && this.getError() != null;
    }

    boolean isCancelled();

    boolean isDone();

    boolean isError();

    Error getError();
}
