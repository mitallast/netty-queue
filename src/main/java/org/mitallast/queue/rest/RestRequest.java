package org.mitallast.queue.rest;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface RestRequest {
    HttpMethod getHttpMethod();

    HttpHeaders getHttpHeaders();

    String getQueryPath();

    String getUri();

    Map<String, List<String>> getParamMap();

    String param(String param);

    boolean hasParam(String param);

    InputStream getInputStream();

    ByteBuf content();
}
