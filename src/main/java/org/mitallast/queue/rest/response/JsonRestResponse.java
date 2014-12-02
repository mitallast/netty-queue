package org.mitallast.queue.rest.response;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.rest.transport.HttpResponse;

public class JsonRestResponse extends HttpResponse {
    public JsonRestResponse(HttpResponseStatus status, ByteBuf buffer) {
        super(status, buffer);
    }
}
