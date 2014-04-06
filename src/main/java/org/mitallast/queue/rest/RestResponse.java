package org.mitallast.queue.rest;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.OutputStream;

public interface RestResponse {
    boolean hasBuffer();

    ByteBuf getBuffer();

    OutputStream getOutputStream();

    HttpHeaders getHeaders();

    void addLocationHeader(String url);

    void setResponseStatus(int value);

    HttpResponseStatus getResponseStatus();

    void setResponseStatus(HttpResponseStatus status);

    void setResponseCreated();

    void setResponseNoContent();

    CharSequence getContentType();

    void setContentType(CharSequence contentType);

    Throwable getException();

    void setException(Throwable exception);

    boolean hasException();
}
