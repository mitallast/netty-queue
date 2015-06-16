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
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.TransportClient;

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
    public SmartFuture<PushResponse> pushRequest(PushRequest request) {
        return transportClient.send(request, PushResponse.mapper);
    }

    @Override
    public SmartFuture<PopResponse> popRequest(PopRequest request) {
        return transportClient.send(request, PopResponse.mapper);
    }

    @Override
    public SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) {
        return transportClient.send(request, QueueStatsResponse.mapper);
    }

    @Override
    public SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) {
        return transportClient.send(request, DeleteResponse.mapper);
    }

    @Override
    public SmartFuture<GetResponse> getRequest(GetRequest request) {
        return transportClient.send(request, GetResponse.mapper);
    }

    @Override
    public SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) {
        return transportClient.send(request, PeekQueueResponse.mapper);
    }
}