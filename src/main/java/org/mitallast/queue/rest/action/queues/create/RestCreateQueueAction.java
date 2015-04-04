package org.mitallast.queue.rest.action.queues.create;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.QueueAlreadyExistsException;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;
import org.mitallast.queue.rest.response.StringRestResponse;

public class RestCreateQueueAction extends BaseRestHandler {

    @Inject
    public RestCreateQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.PUT, "/{queue}", this);
        controller.registerHandler(HttpMethod.POST, "/{queue}", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(request.param("queue").toString());
        createQueueRequest.setSettings(ImmutableSettings.EMPTY);

        client.queues().createQueue(createQueueRequest, new ActionListener<CreateQueueResponse>() {
            @Override
            public void onResponse(CreateQueueResponse response) {
                session.sendResponse(new StatusRestResponse(HttpResponseStatus.CREATED));
            }

            @Override
            public void onFailure(Throwable e) {
                if (e instanceof QueueAlreadyExistsException) {
                    session.sendResponse(new StringRestResponse(HttpResponseStatus.CONFLICT, "Queue already exists"));
                } else {
                    session.sendResponse(e);
                }
            }
        });
    }
}
