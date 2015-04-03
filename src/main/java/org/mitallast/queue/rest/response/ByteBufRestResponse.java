package org.mitallast.queue.rest.response;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.rest.transport.HttpResponse;

public class ByteBufRestResponse extends HttpResponse {
    public ByteBufRestResponse(HttpResponseStatus status, ByteBuf buffer) {
        super(status, buffer);
    }
}
