package org.mitallast.queue.common.concurrent.futures;

public interface SmartFuture<Type> extends ListenableFuture<Type>, FutureResult<Type, Throwable> {

    /**
     * Данный метод вызывается при получении значения в фьючере
     *
     * @param result значение которое установлено во фьючер
     */
    public void invoke(Type result);

    /**
     * Данный метод вызывается при установке ошибки выполнения фьючера
     *
     * @param ex ошибка
     */
    public void invokeException(Throwable ex);
}
