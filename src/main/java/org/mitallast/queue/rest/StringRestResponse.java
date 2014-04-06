package org.mitallast.queue.rest;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.transport.TransportResponse;

import java.io.IOException;
import java.io.OutputStream;

public class StringRestResponse extends TransportResponse {

    public StringRestResponse(HttpResponseStatus status) {
        setResponseStatus(status);
    }

    public StringRestResponse(HttpResponseStatus status, String message) {
        setResponseStatus(status);
        try {
            try (OutputStream stream = getOutputStream()) {
                stream.write(message.getBytes());
            }
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }
}
