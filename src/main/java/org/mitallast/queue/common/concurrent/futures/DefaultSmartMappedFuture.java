package org.mitallast.queue.common.concurrent.futures;

public class DefaultSmartMappedFuture<From, To> extends DefaultSmartFuture<To> implements SmartMappedFuture<From, To> {

    private final Mapper<From, To> mapper;

    public DefaultSmartMappedFuture(Mapper<From, To> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void onResponse(From message) {
        try {
            invoke(mapper.map(message));
        } catch (Exception e) {
            invokeException(e);
        }
    }

    @Override
    public void onFailure(Throwable e) {
        invokeException(e);
    }
}
