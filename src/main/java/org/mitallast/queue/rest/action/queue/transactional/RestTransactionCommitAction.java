package org.mitallast.queue.rest.action.queue.transactional;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.UUIDRestResponse;

public class RestTransactionCommitAction extends BaseRestHandler {

    @Inject
    public RestTransactionCommitAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.PUT, "/{queue}/{transaction}/commit", this);
        controller.registerHandler(HttpMethod.POST, "/{queue}/{transaction}/commit", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        TransactionCommitRequest commitRequest = TransactionCommitRequest.builder()
            .setQueue(request.param("queue").toString())
            .setTransactionUUID(UUIDs.fromString(request.param("transaction")))
            .build();

        client.queue().transactional().commitRequest(commitRequest).whenComplete((response, error) -> {
            if (error == null) {
                session.sendResponse(new UUIDRestResponse(HttpResponseStatus.ACCEPTED, response.transactionUUID()));
            } else {
                session.sendResponse(error);
            }
        });
    }
}
