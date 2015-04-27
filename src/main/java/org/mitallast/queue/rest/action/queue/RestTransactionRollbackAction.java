package org.mitallast.queue.rest.action.queue;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackRequest;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackResponse;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.UUIDRestResponse;

public class RestTransactionRollbackAction extends BaseRestHandler {

    @Inject
    public RestTransactionRollbackAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.DELETE, "/{queue}/{transaction}/commit", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        TransactionRollbackRequest rollbackRequest = new TransactionRollbackRequest();
        rollbackRequest.setQueue(request.param("queue").toString());
        rollbackRequest.setTransactionUUID(UUIDs.fromString(request.param("transaction")));

        client.queue().transactional().rollbackRequest(rollbackRequest, new Listener<TransactionRollbackResponse>() {
            @Override
            public void onResponse(TransactionRollbackResponse result) {
                session.sendResponse(new UUIDRestResponse(HttpResponseStatus.ACCEPTED, result.getTransactionUUID()));
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
