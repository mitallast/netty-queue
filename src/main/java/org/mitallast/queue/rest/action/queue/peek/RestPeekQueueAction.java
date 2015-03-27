package org.mitallast.queue.rest.action.queue.peek;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.JsonRestResponse;
import org.mitallast.queue.rest.response.StatusRestResponse;

import java.io.IOException;

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
            public void onResponse(PeekQueueResponse peekQueueResponse) {
                if (peekQueueResponse.getMessage() == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.NO_CONTENT));
                    return;
                }
                QueueMessage queueMessage = peekQueueResponse.getMessage();
                ByteBuf buffer = Unpooled.buffer();
                try {
                    try (XStreamBuilder builder = createBuilder(request, buffer)) {
                        queueMessage.toXStream(builder);
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