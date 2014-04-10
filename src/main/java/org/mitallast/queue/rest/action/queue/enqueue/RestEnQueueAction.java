package org.mitallast.queue.rest.action.queue.enqueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queues.enqueue.EnQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.*;

import java.io.IOException;
import java.io.OutputStream;

public class RestEnQueueAction extends BaseRestHandler {

    public RestEnQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.PUT, "/{queue}/_enqueue", this);
        controller.registerHandler(HttpMethod.POST, "/{queue}/_enqueue", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        EnQueueRequest<String> enQueueRequest = new EnQueueRequest<>();
        enQueueRequest.setQueue(request.param("queue"));

        QueueMessage<String> queueMessage = new QueueMessage<>();
        queueMessage.setMessage(request.getBody());
        enQueueRequest.setMessage(queueMessage);

        client.queue().enQueueRequest(enQueueRequest, new ActionListener<EnQueueResponse>() {

            @Override
            public void onResponse(EnQueueResponse response) {
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
