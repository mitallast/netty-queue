package org.mitallast.queue.rest;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Map;

public interface RestRequest {
    HttpMethod getHttpMethod();

    String getQueryPath();

    String getUri();

    Map<String, CharSequence> getParamMap();

    CharSequence param(String param);

    boolean hasParam(String param);

    ByteBuf content();
}
