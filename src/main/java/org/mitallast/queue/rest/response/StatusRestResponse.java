package org.mitallast.queue.rest.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.transport.TransportResponse;

public class StatusRestResponse extends TransportResponse {
    public StatusRestResponse(HttpResponseStatus status) {
        setResponseStatus(status);
    }
}
