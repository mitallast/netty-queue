package org.mitallast.queue.rest.action.queue.transactional;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.transactional.push.TransactionPushRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamParser;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.action.support.QueueMessageParser;
import org.mitallast.queue.rest.response.UUIDRestResponse;

import java.io.IOException;

public class RestTransactionPushAction extends BaseRestHandler {

    @Inject
    public RestTransactionPushAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.POST, "/{queue}/{transaction}/message", this);
        controller.registerHandler(HttpMethod.PUT, "/{queue}/{transaction}/message", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {

        TransactionPushRequest.Builder builder = TransactionPushRequest.builder()
            .setQueue(request.param("queue").toString())
            .setTransactionUUID(UUIDs.fromString(request.param("transaction")));

        try (XStreamParser parser = createParser(request.content())) {
            QueueMessage message = new QueueMessage();
            QueueMessageParser.parse(message, parser);
            builder.setMessage(message);
        } catch (IOException e) {
            session.sendResponse(e);
            return;
        }

        client.queue().transactional().pushRequest(builder.build()).whenComplete((response, error) -> {
            if (error == null) {
                session.sendResponse(new UUIDRestResponse(HttpResponseStatus.CREATED, response.messageUUID()));
            } else {
                session.sendResponse(error);
            }
        });
    }
}
