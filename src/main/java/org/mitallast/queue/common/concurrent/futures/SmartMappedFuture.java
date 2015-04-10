package org.mitallast.queue.common.concurrent.futures;

import org.mitallast.queue.common.concurrent.Listener;

public interface SmartMappedFuture<From, To> extends Listener<From>, SmartFuture<To> {
}
