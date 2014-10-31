package org.mitallast.queue.rest;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

public interface RestResponse {
    ByteBuf getBuffer();

    HttpHeaders getHeaders();

    HttpResponseStatus getResponseStatus();
}
