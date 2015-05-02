package org.mitallast.queue.rest.action.queue;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamParser;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.action.support.QueueMessageParser;
import org.mitallast.queue.rest.response.StringRestResponse;
import org.mitallast.queue.rest.response.UUIDRestResponse;

import java.io.IOException;

public class RestPushAction extends BaseRestHandler {

    @Inject
    public RestPushAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.POST, "/{queue}/message", this);
        controller.registerHandler(HttpMethod.PUT, "/{queue}/message", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {

        final PushRequest pushRequest = new PushRequest();
        pushRequest.setQueue(request.param("queue").toString());

        try (XStreamParser parser = createParser(request.content())) {
            QueueMessage message = new QueueMessage();
            QueueMessageParser.parse(message, parser);
            pushRequest.setMessage(message);
        } catch (IOException e) {
            session.sendResponse(e);
            return;
        }

        client.queue().pushRequest(pushRequest, new Listener<PushResponse>() {

            @Override
            public void onResponse(PushResponse response) {
                session.sendResponse(new UUIDRestResponse(HttpResponseStatus.CREATED, response.getMessageUUID()));
            }

            @Override
            public void onFailure(Throwable e) {
                if (e instanceof QueueMessageUuidDuplicateException) {
                    session.sendResponse(new StringRestResponse(HttpResponseStatus.CONFLICT, "Message already exists"));
                } else {
                    session.sendResponse(e);
                }
            }
        });
    }
}
