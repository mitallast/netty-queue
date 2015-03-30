package org.mitallast.queue.common.concurrent.futures;

public final class Futures {
    public static <Type> SmartFuture<Type> future() {
        return new DefaultSmartFuture<>();
    }

    public static <Type> SmartFuture<Type> future(Type value) {
        return new SuccessImmediatelyFuture<>(value);
    }

    public static <Type> SmartFuture<Type> future(Throwable value) {
        return new FailImmediatelyFuture<>(value);
    }
}
