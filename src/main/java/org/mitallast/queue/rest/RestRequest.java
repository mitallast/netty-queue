package org.mitallast.queue.rest;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import java.io.InputStream;
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

    String getPath();

    String getBaseUrl();

    String getUrl();

    public Map<String, String> getQueryStringMap();

    InputStream getInputStream();
}
