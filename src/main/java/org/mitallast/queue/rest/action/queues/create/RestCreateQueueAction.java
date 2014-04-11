package org.mitallast.queue.rest.action.queues.create;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueType;
import org.mitallast.queue.rest.*;

import java.io.IOException;
import java.io.OutputStream;

public class RestCreateQueueAction extends BaseRestHandler {

    @Inject
    public RestCreateQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.PUT, "/{queue}", this);
        controller.registerHandler(HttpMethod.POST, "/{queue}", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(request.param("queue"));
        createQueueRequest.setSettings(ImmutableSettings.EMPTY);

        if (request.param("type") != null) {
            createQueueRequest.setType(QueueType.valueOf(request.param("type").toUpperCase()));
        }

        client.queues().createQueue(createQueueRequest, new ActionListener<CreateQueueResponse>() {
            @Override
            public void onResponse(CreateQueueResponse response) {
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
