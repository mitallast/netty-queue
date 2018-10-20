package org.mitallast.queue.rest.netty.codec;

import io.netty.util.Recycler;

import java.util.HashMap;

public class HttpHeaders extends HashMap<HttpHeaderName, AsciiString> {

    private static final Recycler<HttpHeaders> recycler = new Recycler<>() {
        @Override
        protected HttpHeaders newObject(Handle<HttpHeaders> handle) {
            return new HttpHeaders(handle);
        }
    };

    public static HttpHeaders newInstance() {
        return recycler.get();
    }

    private final Recycler.Handle<HttpHeaders> handle;

    private HttpHeaders(Recycler.Handle<HttpHeaders> handle) {
        super();
        this.handle = handle;
    }

    public void release() {
        forEach((key, value) -> value.release());
        handle.recycle(this);
    }
}
