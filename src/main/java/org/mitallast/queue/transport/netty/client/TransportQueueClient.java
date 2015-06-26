package org.mitallast.queue.transport.netty.client;

import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.action.queue.pop.PopRequest;
import org.mitallast.queue.action.queue.pop.PopResponse;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.client.QueueTransactionalClient;
import org.mitallast.queue.transport.TransportClient;

import java.util.concurrent.CompletableFuture;

public class TransportQueueClient implements QueueClient {

    private final TransportClient transportClient;
    private final TransportQueueTransactionalClient queueTransactionalClient;

    public TransportQueueClient(TransportClient transportClient) {
        this(transportClient, new TransportQueueTransactionalClient(transportClient));
    }

    public TransportQueueClient(TransportClient transportClient, TransportQueueTransactionalClient queueTransactionalClient) {
        this.transportClient = transportClient;
        this.queueTransactionalClient = queueTransactionalClient;
    }

    @Override
    public QueueTransactionalClient transactional() {
        return queueTransactionalClient;
    }

    @Override
    public CompletableFuture<PushResponse> pushRequest(PushRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<PopResponse> popRequest(PopRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<DeleteResponse> deleteRequest(DeleteRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<GetResponse> getRequest(GetRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) {
        return transportClient.send(request);
    }
}