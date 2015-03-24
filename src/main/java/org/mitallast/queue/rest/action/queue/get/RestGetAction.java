package org.mitallast.queue.rest.action.queue.get;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.UUIDs;
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

public class RestGetAction extends BaseRestHandler {

    @Inject
    public RestGetAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.HEAD, "/{queue}/message/{uuid}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        GetRequest getRequest = new GetRequest();
        getRequest.setQueue(request.param("queue"));
        String uuid = request.param("uuid");
        if (uuid != null) {
            getRequest.setUuid(UUIDs.fromString(uuid));
        }

        client.queue().getRequest(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.getMessage() == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.NOT_FOUND));
                    return;
                }
                QueueMessage queueMessage = getResponse.getMessage();
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