package org.mitallast.queue.rest.netty.codec;

public enum HttpVersion {
    HTTP_1_0("HTTP/1.0"),
    HTTP_1_1("HTTP/1.1");

    private final AsciiString ascii;

    HttpVersion(String ascii) {
        this.ascii = AsciiString.of(ascii);
    }

    public AsciiString ascii() {
        return ascii;
    }
}
