package org.mitallast.queue.transport;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.netty.ResponseMapper;

public interface TransportClient extends Client {

    <Request extends ActionRequest, Response extends ActionResponse>
    SmartFuture<Response> send(String actionName, Request request, ResponseMapper<Response> mapper);
}