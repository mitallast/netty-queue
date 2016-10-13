package org.mitallast.queue.rest;

import io.netty.buffer.ByteBufAllocator;

public interface RestSession {

    ByteBufAllocator alloc();

    void sendResponse(RestResponse response);

    void sendResponse(Throwable response);
}
