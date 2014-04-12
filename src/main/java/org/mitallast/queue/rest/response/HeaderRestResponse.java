package org.mitallast.queue.rest.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.transport.TransportResponse;

public class HeaderRestResponse extends TransportResponse {

    public HeaderRestResponse(HttpResponseStatus status, String header, Object value) {
        setResponseStatus(status);
        getHeaders().set(header, value);
    }

    public HeaderRestResponse(String header, Object value) {
        getHeaders().set(header, value);
    }
}
