package org.mitallast.queue.transport;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.client.Client;

import java.util.concurrent.CompletableFuture;

public interface TransportClient extends Client {

    <Request extends ActionRequest, Response extends ActionResponse>
    CompletableFuture<Response> send(Request request);
}