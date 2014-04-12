package org.mitallast.queue.rest.response;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.transport.TransportResponse;

public class JsonRestResponse extends TransportResponse {
    public JsonRestResponse(HttpResponseStatus status) {
        setResponseStatus(status);
        getHeaders().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
    }
}
