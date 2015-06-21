package org.mitallast.queue.rest.action.queues;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueueMissingException;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;
import org.mitallast.queue.rest.response.StringRestResponse;

public class RestDeleteQueueAction extends BaseRestHandler {

    @Inject
    public RestDeleteQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.DELETE, "/{queue}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
            .setQueue(request.param("queue").toString())
            .setReason(request.param("reason").toString())
            .build();

        client.queues().deleteQueue(deleteQueueRequest, new Listener<DeleteQueueResponse>() {
            @Override
            public void onResponse(DeleteQueueResponse response) {
                if (response.isDeleted()) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.ACCEPTED));
                } else {
                    if (response.error() instanceof QueueMissingException) {
                        session.sendResponse(new StringRestResponse(
                            HttpResponseStatus.NOT_FOUND,
                            response.error().getMessage()));
                    }
                }
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
