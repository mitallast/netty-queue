package org.mitallast.queue.rest.netty.codec;

public enum HttpMethod {
    OPTIONS("options"),
    GET("get"),
    HEAD("head"),
    POST("post"),
    PUT("put"),
    PATCH("patch"),
    DELETE("delete"),
    TRACE("trace"),
    CONNECT("connect");

    private final AsciiString ascii;

    HttpMethod(String method) {
        this.ascii = AsciiString.of(method);
    }

    public AsciiString ascii() {
        return ascii;
    }
}
