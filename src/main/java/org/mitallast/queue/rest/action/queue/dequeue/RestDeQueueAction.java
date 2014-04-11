package org.mitallast.queue.rest.action.queue.dequeue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.*;

import java.io.IOException;
import java.io.OutputStream;

public class RestDeQueueAction extends BaseRestHandler {

    @Inject
    public RestDeQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/{queue}/_dequeue", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        DeQueueRequest deQueueRequest = new DeQueueRequest();
        deQueueRequest.setQueue(request.param("queue"));

        client.queue().deQueueRequest(deQueueRequest, new ActionListener<DeQueueResponse>() {
            @Override
            public void onResponse(DeQueueResponse deQueueResponse) {
                if (deQueueResponse.getMessage() == null) {
                    session.sendResponse(new StringRestResponse(HttpResponseStatus.NO_CONTENT));
                    return;
                }
                JsonRestResponse restResponse = new JsonRestResponse(HttpResponseStatus.OK);
                JsonFactory factory = new JsonFactory();
                try (OutputStream stream = restResponse.getOutputStream()) {
                    JsonGenerator generator = factory.createGenerator(stream);
                    generator.writeStartObject();
                    generator.writeFieldName("message");

                    generator.writeObject(deQueueResponse.getMessage().getMessage());
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
