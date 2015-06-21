package org.mitallast.queue.rest.action.queue;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;
import org.mitallast.queue.rest.response.StatusRestResponse;

import java.io.IOException;

public class RestGetAction extends BaseRestHandler {

    @Inject
    public RestGetAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.HEAD, "/{queue}/message/{uuid}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        GetRequest.Builder builder = GetRequest.builder();
        builder.setQueue(request.param("queue").toString());
        CharSequence uuid = request.param("uuid");
        if (uuid != null) {
            builder.setUuid(UUIDs.fromString(uuid));
        }

        client.queue().getRequest(builder.build(), new Listener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.message() == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.NOT_FOUND));
                    return;
                }
                QueueMessage queueMessage = getResponse.message();
                ByteBuf buffer = Unpooled.buffer();
                try {
                    try (XStreamBuilder builder = createBuilder(request, buffer)) {
                        queueMessage.toXStream(builder);
                    }
                    session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
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