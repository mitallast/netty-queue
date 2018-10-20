package org.mitallast.queue.rest.netty.codec;

public enum HttpHeaderValue {
    APPLICATION_JSON("application/json"),
    APPLICATION_X_WWW_FORM_URLENCODED("application/x-www-form-urlencoded"),
    APPLICATION_OCTET_STREAM("application/octet-stream"),
    ATTACHMENT("attachment"),
    BASE64("base64"),
    BINARY("binary"),
    BOUNDARY("boundary"),
    BYTES("bytes"),
    CHARSET("charset"),
    CHUNKED("chunked"),
    CLOSE("close"),
    COMPRESS("compress"),
    CONTINUE("100-continue"),
    DEFLATE("deflate"),
    X_DEFLATE("x-deflate"),
    FILE("file"),
    FILENAME("filename"),
    FORM_DATA("form-data"),
    GZIP("gzip"),
    GZIP_DEFLATE("gzip,deflate"),
    X_GZIP("x-gzip"),
    IDENTITY("identity"),
    KEEP_ALIVE("keep-alive"),
    MAX_AGE("max-age"),
    MAX_STALE("max-stale"),
    MIN_FRESH("min-fresh"),
    MULTIPART_FORM_DATA("multipart/form-data"),
    MULTIPART_MIXED("multipart/mixed"),
    MUST_REVALIDATE("must-revalidate"),
    NAME("name"),
    NO_CACHE("no-cache"),
    NO_STORE("no-store"),
    NO_TRANSFORM("no-transform"),
    NONE("none"),
    ZERO("0"),
    ONLY_IF_CACHED("only-if-cached"),
    PRIVATE("private"),
    PROXY_REVALIDATE("proxy-revalidate"),
    PUBLIC("public"),
    QUOTED_PRINTABLE("quoted-printable"),
    S_MAXAGE("s-maxage"),
    TEXT_PLAIN("text/plain"),
    TRAILERS("trailers"),
    UPGRADE("upgrade"),
    WEBSOCKET("websocket");


    private final AsciiString ascii;

    HttpHeaderValue(String ascii) {
        this.ascii = AsciiString.of(ascii);
    }

    public AsciiString ascii() {
        return ascii;
    }
}
