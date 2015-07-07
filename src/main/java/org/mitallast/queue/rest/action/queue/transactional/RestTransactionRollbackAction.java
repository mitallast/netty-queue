package org.mitallast.queue.rest.action.queue.transactional;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackRequest;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackResponse;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.UUIDRestResponse;
import org.mitallast.queue.transport.TransportService;

public class RestTransactionRollbackAction extends BaseRestHandler {
    private final TransportService transportService;

    @Inject
    public RestTransactionRollbackAction(Settings settings, RestController controller, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        controller.registerHandler(HttpMethod.DELETE, "/{queue}/{transaction}/commit", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        TransactionRollbackRequest rollbackRequest = TransactionRollbackRequest.builder()
            .setQueue(request.param("queue").toString())
            .setTransactionUUID(UUIDs.fromString(request.param("transaction")))
            .build();

        transportService.client().<TransactionRollbackRequest, TransactionRollbackResponse>send(rollbackRequest)
            .whenComplete((response, error) -> {
                if (error == null) {
                    session.sendResponse(new UUIDRestResponse(HttpResponseStatus.ACCEPTED, response.transactionUUID()));
                } else {
                    session.sendResponse(error);
                }
            });
    }
}
