package org.mitallast.queue.rest.action.queue;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;
import org.mitallast.queue.transport.TransportService;

public class RestDeleteAction extends BaseRestHandler {
    private final TransportService transportService;

    @Inject
    public RestDeleteAction(Settings settings, RestController controller, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        controller.registerHandler(HttpMethod.DELETE, "/{queue}/message", this);
        controller.registerHandler(HttpMethod.DELETE, "/{queue}/message/{uuid}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        DeleteRequest.Builder builder = DeleteRequest.builder();
        builder.setQueue(request.param("queue").toString());

        CharSequence uuid = request.param("uuid");
        if (uuid != null) {
            builder.setMessageUUID(UUIDs.fromString(uuid));
        }

        transportService.client().send(builder.build())
            .whenComplete((deleteResponse, error) -> {
                if (error == null) {
                    session.sendResponse(new StatusRestResponse(HttpResponseStatus.ACCEPTED));
                } else {
                    session.sendResponse(error);
                }
            });
    }
}