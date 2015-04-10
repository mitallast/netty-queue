package org.mitallast.queue.common.concurrent.futures;

import org.mitallast.queue.common.concurrent.Listener;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;

public interface SmartFuture<Type> extends Future<Type> {

    /**
     * Данный метод вызывается при получении значения в фьючере
     *
     * @param result значение которое установлено во фьючер
     */
    void invoke(@Nonnull Type result);

    /**
     * Данный метод вызывается при установке ошибки выполнения фьючера
     *
     * @param ex ошибка
     */
    void invokeException(@Nonnull Throwable ex);

    void on(Listener<Type> listener);

    default <Map> SmartFuture<Map> map(Mapper<Type, Map> mapper) {
        SmartMappedFuture<Type, Map> mappedFuture = Futures.mappedFuture(mapper);
        on(mappedFuture);
        return mappedFuture;
    }
}
