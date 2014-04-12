package org.mitallast.queue.rest.action.queue.enqueue;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.HeaderRestResponse;
import org.mitallast.queue.rest.support.Headers;

public class RestEnQueueAction extends BaseRestHandler {

    @Inject
    public RestEnQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.POST, "/{queue}/message", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        final EnQueueRequest<String> enQueueRequest = new EnQueueRequest<>();
        enQueueRequest.setQueue(request.param("queue"));

        QueueMessage<String> queueMessage = new QueueMessage<>();
        queueMessage.setMessage(request.getBody());
        enQueueRequest.setMessage(queueMessage);

        client.queue().enQueueRequest(enQueueRequest, new ActionListener<EnQueueResponse>() {

            @Override
            public void onResponse(EnQueueResponse response) {
                session.sendResponse(new HeaderRestResponse(HttpResponseStatus.CREATED, Headers.MESSAGE_INDEX, response.getIndex()));
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
