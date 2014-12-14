package org.mitallast.queue.rest.action;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.JsonRestResponse;

import java.io.IOException;
import java.io.OutputStream;

public class RestIndexAction extends BaseRestHandler {

    @Inject
    public RestIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/", this);
        controller.registerHandler(HttpMethod.HEAD, "/", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        ByteBuf buffer = Unpooled.buffer();
        try (OutputStream stream = new ByteBufOutputStream(buffer)) {
            try (JsonGenerator generator = createGenerator(request, stream)) {
                generator.writeStartObject();
                generator.writeStringField("message", "You now, for queue");
                generator.writeEndObject();
            }
            session.sendResponse(new JsonRestResponse(HttpResponseStatus.OK, buffer));
        } catch (IOException e) {
            session.sendResponse(e);
        }
    }
}
