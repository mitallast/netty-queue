package org.mitallast.queue.common.concurrent.futures;

import javax.annotation.Nonnull;

public interface SmartFuture<Type> extends ListenableFuture<Type>, FutureResult<Type, Throwable> {

    /**
     * Данный метод вызывается при получении значения в фьючере
     *
     * @param result значение которое установлено во фьючер
     */
    public void invoke(@Nonnull Type result);

    /**
     * Данный метод вызывается при установке ошибки выполнения фьючера
     *
     * @param ex ошибка
     */
    public void invokeException(@Nonnull Throwable ex);
}
