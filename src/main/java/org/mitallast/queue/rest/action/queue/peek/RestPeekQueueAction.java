package org.mitallast.queue.rest.action.queue.peek;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.JsonRestResponse;
import org.mitallast.queue.rest.response.StatusRestResponse;

import java.io.IOException;
import java.io.OutputStream;

public class RestPeekQueueAction extends BaseRestHandler {

    @Inject
    public RestPeekQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.HEAD, "/{queue}/message", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        PeekQueueRequest peekQueueRequest = new PeekQueueRequest();
        peekQueueRequest.setQueue(request.param("queue"));

        client.queue().peekQueueRequest(peekQueueRequest, new ActionListener<PeekQueueResponse>() {
            @Override
            public void onResponse(PeekQueueResponse deQueueResponse) {
                if (deQueueResponse.getMessage() == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.NO_CONTENT));
                    return;
                }
                JsonRestResponse restResponse = new JsonRestResponse(HttpResponseStatus.OK);
                try (OutputStream stream = restResponse.getOutputStream()) {
                    JsonGenerator generator = getGenerator(request, stream);
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
