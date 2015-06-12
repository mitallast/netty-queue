package org.mitallast.queue.transport.netty.client;

import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitAction;
import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitRequest;
import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitResponse;
import org.mitallast.queue.action.queue.transactional.delete.TransactionDeleteAction;
import org.mitallast.queue.action.queue.transactional.delete.TransactionDeleteRequest;
import org.mitallast.queue.action.queue.transactional.delete.TransactionDeleteResponse;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopAction;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopRequest;
import org.mitallast.queue.action.queue.transactional.pop.TransactionPopResponse;
import org.mitallast.queue.action.queue.transactional.push.TransactionPushAction;
import org.mitallast.queue.action.queue.transactional.push.TransactionPushRequest;
import org.mitallast.queue.action.queue.transactional.push.TransactionPushResponse;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackAction;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackRequest;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackResponse;
import org.mitallast.queue.client.QueueTransactionalClient;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.TransportClient;

public class TransportQueueTransactionalClient implements QueueTransactionalClient {

    private final TransportClient transportClient;

    public TransportQueueTransactionalClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public SmartFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request) {
        return transportClient.send(TransactionCommitAction.actionName, request, TransactionCommitResponse.mapper);
    }

    @Override
    public SmartFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request) {
        return transportClient.send(TransactionDeleteAction.actionName, request, TransactionDeleteResponse.mapper);
    }

    @Override
    public SmartFuture<TransactionPopResponse> popRequest(TransactionPopRequest request) {
        return transportClient.send(TransactionPopAction.actionName, request, TransactionPopResponse.mapper);
    }

    @Override
    public SmartFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request) {
        return transportClient.send(TransactionPushAction.actionName, request, TransactionPushResponse.mapper);
    }

    @Override
    public SmartFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request) {
        return transportClient.send(TransactionRollbackAction.actionName, request, TransactionRollbackResponse.mapper);
    }
}
