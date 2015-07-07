package org.mitallast.queue.rest.action.queue.transactional;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopRequest;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopResponse;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;
import org.mitallast.queue.rest.response.StatusRestResponse;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;

public class RestTransactionPopAction extends BaseRestHandler {
    private final TransportService transportService;

    @Inject
    public RestTransactionPopAction(Settings settings, RestController controller, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        controller.registerHandler(HttpMethod.GET, "/{queue}/{transaction}/message", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        TransactionPopRequest popRequest = TransactionPopRequest.builder()
            .setQueue(request.param("queue").toString())
            .setTransactionUUID(UUIDs.fromString(request.param("transaction")))
            .build();

        transportService.client().<TransactionPopRequest, TransactionPopResponse>send(popRequest)
            .whenComplete((response, error) -> {
                if (error == null) {
                    if (response.message() == null) {
                        session.sendResponse(new StatusRestResponse(HttpResponseStatus.NO_CONTENT));
                        return;
                    }
                    QueueMessage queueMessage = response.message();
                    ByteBuf buffer = Unpooled.buffer();
                    try {
                        try (XStreamBuilder builder = createBuilder(request, buffer)) {
                            queueMessage.toXStream(builder);
                        }
                        session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
                    } catch (IOException e) {
                        session.sendResponse(e);
                    }
                } else {
                    session.sendResponse(error);
                }
            });
    }
}