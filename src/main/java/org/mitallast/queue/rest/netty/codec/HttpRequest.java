package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;

public class HttpRequest {
    private final HttpMethod method;
    private final HttpVersion version;
    private final AsciiString uri;
    private final HttpHeaders headers;
    private final ByteBuf body;

    public HttpRequest(HttpMethod method,
                       HttpVersion version,
                       AsciiString uri,
                       HttpHeaders headers, ByteBuf body) {
        this.method = method;
        this.version = version;
        this.uri = uri;
        this.headers = headers;
        this.body = body;
    }

    public HttpMethod method() {
        return method;
    }

    public HttpVersion version() {
        return version;
    }

    public AsciiString uri() {
        return uri;
    }

    public HttpHeaders headers() {
        return headers;
    }

    public ByteBuf body() {
        return body;
    }

    public void release() {
        uri.release();
        body.release();
        headers.release();
    }
}
