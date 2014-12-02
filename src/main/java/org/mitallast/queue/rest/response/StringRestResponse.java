package org.mitallast.queue.rest.response;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.rest.transport.HttpResponse;

import java.nio.charset.Charset;

public class StringRestResponse extends HttpResponse {

    public StringRestResponse(HttpResponseStatus status, String message) {
        super(status, Unpooled.wrappedBuffer(message.getBytes(Charset.forName("UTF-8"))));
    }
}
