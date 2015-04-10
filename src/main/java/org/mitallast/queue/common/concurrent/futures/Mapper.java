package org.mitallast.queue.common.concurrent.futures;

public interface Mapper<From, To> {

    To map(From value) throws Exception;
}
