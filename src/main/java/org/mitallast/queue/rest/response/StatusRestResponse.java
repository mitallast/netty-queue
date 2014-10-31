package org.mitallast.queue.rest.response;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.transport.TransportResponse;

public class StatusRestResponse extends TransportResponse {
    public StatusRestResponse(HttpResponseStatus status) {
        super(status, Unpooled.EMPTY_BUFFER);
    }
}
