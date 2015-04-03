package org.mitallast.queue.rest.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.rest.RestResponse;

public class HttpResponse implements RestResponse {
    private final HttpResponseStatus responseStatus;
    private final ByteBuf buffer;

    public HttpResponse() {
        this(HttpResponseStatus.OK);
    }

    public HttpResponse(HttpResponseStatus responseStatus) {
        this(responseStatus, Unpooled.EMPTY_BUFFER);
    }

    public HttpResponse(HttpResponseStatus responseStatus, ByteBuf buffer) {
        this.responseStatus = responseStatus;
        this.buffer = buffer;
    }

    @Override
    public ByteBuf getBuffer() {
        return buffer;
    }

    @Override
    public HttpResponseStatus getResponseStatus() {
        return responseStatus;
    }
}
