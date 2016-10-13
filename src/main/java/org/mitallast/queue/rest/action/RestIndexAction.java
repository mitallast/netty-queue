package org.mitallast.queue.rest.action;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.common.xstream.XStreamString;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class RestIndexAction extends BaseRestHandler {

    private final static XStreamString MESSAGE = new XStreamString("message");
    private final static XStreamString MESSAGE_TEXT = new XStreamString("You now, for queue");

    @Inject
    public RestIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(HttpMethod.GET, "/", this);
        controller.registerHandler(HttpMethod.HEAD, "/", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        ByteBuf buffer = session.alloc().directBuffer();
        try {
            try (XStreamBuilder builder = createBuilder(request, buffer)) {
                builder.writeStartObject();
                builder.writeFieldName(MESSAGE);
                builder.writeString(MESSAGE_TEXT);
                builder.writeEndObject();
            }
            session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
        } catch (IOException e) {
            session.sendResponse(e);
        }
    }
}
