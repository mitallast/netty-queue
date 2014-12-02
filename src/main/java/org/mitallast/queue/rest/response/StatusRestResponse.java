package org.mitallast.queue.rest.response;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.rest.transport.HttpResponse;

public class StatusRestResponse extends HttpResponse {
    public StatusRestResponse(HttpResponseStatus status) {
        super(status, Unpooled.EMPTY_BUFFER);
    }
}
