package org.mitallast.queue.transport.netty.client;

import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitRequest;
import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitResponse;
import org.mitallast.queue.action.queue.transactional.delete.TransactionDeleteRequest;
import org.mitallast.queue.action.queue.transactional.delete.TransactionDeleteResponse;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopRequest;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopResponse;
import org.mitallast.queue.action.queue.transactional.push.TransactionPushRequest;
import org.mitallast.queue.action.queue.transactional.push.TransactionPushResponse;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackRequest;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackResponse;
import org.mitallast.queue.client.QueueTransactionalClient;
import org.mitallast.queue.transport.TransportClient;

import java.util.concurrent.CompletableFuture;

public class TransportQueueTransactionalClient implements QueueTransactionalClient {

    private final TransportClient transportClient;

    public TransportQueueTransactionalClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public CompletableFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<TransactionPopResponse> popRequest(TransactionPopRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request) {
        return transportClient.send(request);
    }

    @Override
    public CompletableFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request) {
        return transportClient.send(request);
    }
}
