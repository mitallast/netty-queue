package org.mitallast.queue.common.concurrent.futures;

public interface FutureResult<Type, Error> {

    Type getOrNull();

    default Type getOrDefault(Type defValue) {
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
