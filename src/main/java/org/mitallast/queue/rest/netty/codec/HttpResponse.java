package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;

public class HttpResponse {
    private final HttpVersion version;
    private final HttpResponseStatus status;
    private final HttpHeaders headers;
    private final ByteBuf content;

    public HttpResponse(HttpVersion version, HttpResponseStatus status, HttpHeaders headers, ByteBuf content) {
        this.version = version;
        this.status = status;
        this.headers = headers;
        this.content = content;
    }

    public HttpVersion version() {
        return version;
    }

    public HttpResponseStatus status() {
        return status;
    }

    public HttpHeaders headers() {
        return headers;
    }

    public ByteBuf content() {
        return content;
    }

    public void release() {
        headers.release();
        content.release();
    }
}
