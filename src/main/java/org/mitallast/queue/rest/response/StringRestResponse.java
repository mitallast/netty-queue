package org.mitallast.queue.rest.response;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.transport.TransportResponse;

import java.io.IOException;
import java.io.OutputStream;

public class StringRestResponse extends TransportResponse {

    public StringRestResponse(HttpResponseStatus status) {
        setResponseStatus(status);
        getHeaders().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
    }

    public StringRestResponse(HttpResponseStatus status, String message) {
        setResponseStatus(status);
        getHeaders().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
        try {
            try (OutputStream stream = getOutputStream()) {
                stream.write(message.getBytes());
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }
}
