package org.mitallast.queue.rest.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.rest.transport.HttpResponse;

import java.util.UUID;

public class UUIDRestResponse extends HttpResponse {
    public UUIDRestResponse(HttpResponseStatus status, UUID uuid) {
        super(status, UUIDs.toByteBuf(uuid));
    }
}
