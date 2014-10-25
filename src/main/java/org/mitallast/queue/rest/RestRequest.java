package org.mitallast.queue.rest;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface RestRequest {
    HttpMethod getHttpMethod();

    HttpHeaders getHttpHeaders();

    boolean isMethodGet();

    boolean isMethodDelete();

    boolean isMethodPost();

    boolean isMethodPut();

    boolean isMethodHead();

    boolean isMethodOptions();

    String getProtocol();

    String getHost();

    String getQueryPath();

    String getBaseUrl();

    String getUrl();

    Map<String, List<String>> getParamMap();

    String param(String param);

    boolean hasParam(String param);

    InputStream getInputStream();

    String getBody();
}
