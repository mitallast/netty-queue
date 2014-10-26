package org.mitallast.queue.rest.action.queue.get;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
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
import java.util.UUID;

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
            getRequest.setUuid(UUID.fromString(uuid));
        }

        client.queue().getRequest(getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.getMessage() == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.NO_CONTENT));
                    return;
                }
                JsonRestResponse restResponse = new JsonRestResponse(HttpResponseStatus.OK);
                try (OutputStream stream = restResponse.getOutputStream()) {
                    JsonGenerator generator = getGenerator(request, stream);
                    generator.writeStartObject();
                    if (getResponse.getMessage().getUuid() != null) {
                        generator.writeStringField("uuid", getResponse.getMessage().getUuid().toString());
                    }
                    generator.writeFieldName("message");
                    generator.writeObject(getResponse.getMessage().getMessage());
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