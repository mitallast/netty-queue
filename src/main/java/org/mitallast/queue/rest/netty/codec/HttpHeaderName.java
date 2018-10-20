package org.mitallast.queue.rest.netty.codec;

public enum HttpHeaderName {
    ACCEPT("accept"),
    ACCEPT_CHARSET("accept-charset"),
    ACCEPT_ENCODING("accept-encoding"),
    ACCEPT_LANGUAGE("accept-language"),
    ACCEPT_RANGES("accept-ranges"),
    ACCEPT_PATCH("accept-patch"),

    ACCESS_CONTROL_ALLOW_CREDENTIALS("access-control-allow-credentials"),
    ACCESS_CONTROL_ALLOW_HEADERS("access-control-allow-headers"),
    ACCESS_CONTROL_ALLOW_METHODS("access-control-allow-methods"),
    ACCESS_CONTROL_ALLOW_ORIGIN("access-control-allow-origin"),
    ACCESS_CONTROL_EXPOSE_HEADERS("access-control-expose-headers"),
    ACCESS_CONTROL_MAX_AGE("access-control-max-age"),
    ACCESS_CONTROL_REQUEST_HEADERS("access-control-request-headers"),
    ACCESS_CONTROL_REQUEST_METHOD("access-control-request-method"),
    AGE("age"),
    ALLOW("allow"),
    AUTHORIZATION("authorization"),

    CACHE_CONTROL("cache-control"),
    CONNECTION("connection"),
    CONTENT_BASE("content-base"),
    CONTENT_ENCODING("content-encoding"),
    CONTENT_LANGUAGE("content-language"),
    CONTENT_LENGTH("content-length"),
    CONTENT_LOCATION("content-location"),
    CONTENT_TRANSFER_ENCODING("content-transfer-encoding"),
    CONTENT_DISPOSITION("content-disposition"),
    CONTENT_MD5("content-md5"),
    CONTENT_RANGE("content-range"),
    CONTENT_SECURITY_POLICY("content-security-policy"),
    CONTENT_TYPE("content-type"),
    COOKIE("cookie"),

    DATE("date"),

    ETAG("etag"),
    EXPECT("expect"),
    EXPIRES("expires"),

    FROM("from"),
    HOST("host"),

    IF_MATCH("if-match"),
    IF_MODIFIED_SINCE("if-modified-since"),
    IF_NONE_MATCH("if-none-match"),
    IF_RANGE("if-range"),
    IF_UNMODIFIED_SINCE("if-unmodified-since"),

    KEEP_ALIVE("keep-alive"),

    LAST_MODIFIED("last-modified"),
    LOCATION("location"),

    MAX_FORWARDS("max-forwards"),

    ORIGIN("origin"),

    PRAGMA("pragma"),
    PROXY_AUTHENTICATE("proxy-authenticate"),
    PROXY_AUTHORIZATION("proxy-authorization"),
    PROXY_CONNECTION("proxy-connection"),

    RANGE("range"),
    REFERER("referer"),
    RETRY_AFTER("retry-after"),

    SEC_WEBSOCKET_KEY("sec-websocket-key"),
    SEC_WEBSOCKET_KEY1("sec-websocket-key1"),
    SEC_WEBSOCKET_KEY2("sec-websocket-key2"),
    SEC_WEBSOCKET_LOCATION("sec-websocket-location"),
    SEC_WEBSOCKET_ORIGIN("sec-websocket-origin"),
    SEC_WEBSOCKET_PROTOCOL("sec-websocket-protocol"),
    SEC_WEBSOCKET_VERSION("sec-websocket-version"),
    SEC_WEBSOCKET_ACCEPT("sec-websocket-accept"),
    SEC_WEBSOCKET_EXTENSIONS("sec-websocket-extensions"),

    SERVER("server"),
    SET_COOKIE("set-cookie"),
    SET_COOKIE2("set-cookie2"),

    TE("te"),
    TRAILER("trailer"),
    TRANSFER_ENCODING("transfer-encoding"),

    UPGRADE("upgrade"),
    USER_AGENT("user-agent"),

    VARY("vary"),
    VIA("via"),

    WARNING("warning"),
    WEBSOCKET_LOCATION("websocket-location"),
    WEBSOCKET_ORIGIN("websocket-origin"),
    WEBSOCKET_PROTOCOL("websocket-protocol"),
    WWW_AUTHENTICATE("www-authenticate"),

    X_FRAME_OPTIONS("x-frame-options");

    private final AsciiString ascii;

    HttpHeaderName(String ascii) {
        this.ascii = AsciiString.of(ascii);
    }

    public AsciiString ascii() {
        return ascii;
    }
}
