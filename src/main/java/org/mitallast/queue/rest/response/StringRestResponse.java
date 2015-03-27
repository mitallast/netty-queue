package org.mitallast.queue.rest.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.rest.transport.HttpResponse;

public class StringRestResponse extends HttpResponse {

    public StringRestResponse(HttpResponseStatus status, String message) {
        super(status, Strings.toByteBuf(message));
    }
}
