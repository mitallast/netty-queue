package org.mitallast.queue.rest.action.queues.remove;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.remove.RemoveQueueRequest;
import org.mitallast.queue.action.queues.remove.RemoveQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.*;

import java.io.IOException;
import java.io.OutputStream;

public class RestRemoveQueueAction extends BaseRestHandler {

    @Inject
    public RestRemoveQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.DELETE, "/{queue}", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        RemoveQueueRequest removeQueueRequest = new RemoveQueueRequest();
        removeQueueRequest.setQueue(request.param("queue"));
        removeQueueRequest.setReason(request.param("reason"));

        client.queues().removeQueue(removeQueueRequest, new ActionListener<RemoveQueueResponse>() {
            @Override
            public void onResponse(RemoveQueueResponse response) {
                JsonRestResponse restResponse = new JsonRestResponse(HttpResponseStatus.OK);
                JsonFactory factory = new JsonFactory();
                try (OutputStream stream = restResponse.getOutputStream()) {
                    JsonGenerator generator = factory.createGenerator(stream);
                    generator.writeStartObject();
                    generator.writeFieldName("acknowledged");
                    generator.writeBoolean(response.isAcknowledged());
                    generator.writeEndObject();
                    generator.close();
                    session.sendResponse(restResponse);
                } catch (IOException e) {
                    session.sendResponse(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
