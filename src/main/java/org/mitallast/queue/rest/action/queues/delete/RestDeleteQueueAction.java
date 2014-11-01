package org.mitallast.queue.rest.action.queues.delete;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;

public class RestDeleteQueueAction extends BaseRestHandler {

    @Inject
    public RestDeleteQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.DELETE, "/{queue}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueue(request.param("queue"));
        deleteQueueRequest.setReason(request.param("reason"));

        client.queues().removeQueue(deleteQueueRequest, new ActionListener<DeleteQueueResponse>() {
            @Override
            public void onResponse(DeleteQueueResponse response) {
                session.sendResponse(new StatusRestResponse(HttpResponseStatus.ACCEPTED));
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
