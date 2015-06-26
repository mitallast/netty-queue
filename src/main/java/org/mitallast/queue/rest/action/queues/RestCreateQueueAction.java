package org.mitallast.queue.rest.action.queues;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;

public class RestCreateQueueAction extends BaseRestHandler {

    @Inject
    public RestCreateQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.PUT, "/{queue}", this);
        controller.registerHandler(HttpMethod.POST, "/{queue}", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
            .setQueue(request.param("queue").toString())
            .setSettings(ImmutableSettings.EMPTY)
            .build();
        client.queues().createQueue(createQueueRequest).whenComplete((response, error) -> {
            if (error == null) {
                session.sendResponse(new StatusRestResponse(HttpResponseStatus.CREATED));
            } else {
                session.sendResponse(error);
            }
        });
    }
}
