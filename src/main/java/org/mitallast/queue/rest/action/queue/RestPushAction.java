package org.mitallast.queue.rest.action.queue;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamParser;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.action.support.QueueMessageParser;
import org.mitallast.queue.rest.response.UUIDRestResponse;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;

public class RestPushAction extends BaseRestHandler {
    private final TransportService transportService;

    @Inject
    public RestPushAction(Settings settings, RestController controller, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        controller.registerHandler(HttpMethod.POST, "/{queue}/message", this);
        controller.registerHandler(HttpMethod.PUT, "/{queue}/message", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        PushRequest.Builder builder = PushRequest.builder()
            .setQueue(request.param("queue").toString());

        try (XStreamParser parser = createParser(request.content())) {
            QueueMessage message = new QueueMessage();
            QueueMessageParser.parse(message, parser);
            builder.setMessage(message);
        } catch (IOException e) {
            session.sendResponse(e);
            return;
        }

        transportService.client().<PushRequest, PushResponse>send(builder.build())
            .whenComplete((response, error) -> {
                if (error == null) {
                    session.sendResponse(new UUIDRestResponse(HttpResponseStatus.CREATED, response.messageUUID()));
                } else {
                    session.sendResponse(error);
                }
            });
    }
}
