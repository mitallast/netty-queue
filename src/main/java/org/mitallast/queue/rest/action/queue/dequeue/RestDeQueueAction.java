package org.mitallast.queue.rest.action.queue.dequeue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.JsonRestResponse;
import org.mitallast.queue.rest.response.StatusRestResponse;

import java.io.IOException;
import java.io.OutputStream;

public class RestDeQueueAction extends BaseRestHandler {

    @Inject
    public RestDeQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/{queue}/message", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        DeQueueRequest deQueueRequest = new DeQueueRequest();
        deQueueRequest.setQueue(request.param("queue"));

        client.queue().dequeueRequest(deQueueRequest, new ActionListener<DeQueueResponse>() {
            @Override
            public void onResponse(DeQueueResponse deQueueResponse) {
                if (deQueueResponse.getMessage() == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.NO_CONTENT));
                    return;
                }
                QueueMessage queueMessage = deQueueResponse.getMessage();
                ByteBuf buffer = Unpooled.buffer();
                try (OutputStream stream = new ByteBufOutputStream(buffer)) {
                    try (JsonGenerator generator = createGenerator(request, stream)) {
                        queueMessage.writeTo(generator);
                    }
                    session.sendResponse(new JsonRestResponse(HttpResponseStatus.OK, buffer));
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