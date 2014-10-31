package org.mitallast.queue.rest.response;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.transport.TransportResponse;

public class JsonRestResponse extends TransportResponse {
    public JsonRestResponse(HttpResponseStatus status, ByteBuf buffer) {
        super(status, buffer);
    }
}
