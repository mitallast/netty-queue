package org.mitallast.queue.rest.action.queues.remove;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.remove.RemoveQueueRequest;
import org.mitallast.queue.action.queues.remove.RemoveQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;

public class RestRemoveQueueAction extends BaseRestHandler {

    @Inject
    public RestRemoveQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.DELETE, "/{queue}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        RemoveQueueRequest removeQueueRequest = new RemoveQueueRequest();
        removeQueueRequest.setQueue(request.param("queue"));
        removeQueueRequest.setReason(request.param("reason"));

        client.queues().removeQueue(removeQueueRequest, new ActionListener<RemoveQueueResponse>() {
            @Override
            public void onResponse(RemoveQueueResponse response) {
                session.sendResponse(new StatusRestResponse(HttpResponseStatus.ACCEPTED));
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
