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
import org.mitallast.queue.client.base.QueueTransactionalClient;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.TransportClient;
import org.mitallast.queue.transport.netty.ResponseMapper;

import java.io.IOException;

public class TransportQueueTransactionalClient implements QueueTransactionalClient {

    private final static ResponseMapper<TransactionCommitResponse> COMMIT_RESPONSE_MAPPER = new ResponseMapper<>(TransactionCommitResponse::new);
    private final static ResponseMapper<TransactionDeleteResponse> DELETE_RESPONSE_MAPPER = new ResponseMapper<>(TransactionDeleteResponse::new);
    private final static ResponseMapper<TransactionPopResponse> POP_RESPONSE_MAPPER = new ResponseMapper<>(TransactionPopResponse::new);
    private final static ResponseMapper<TransactionPushResponse> PUSH_RESPONSE_MAPPER = new ResponseMapper<>(TransactionPushResponse::new);
    private final static ResponseMapper<TransactionRollbackResponse> ROLLBACK_RESPONSE_MAPPER = new ResponseMapper<>(TransactionRollbackResponse::new);

    private final TransportClient transportClient;

    public TransportQueueTransactionalClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public SmartFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request) throws IOException {
        return transportClient.send(request, COMMIT_RESPONSE_MAPPER);
    }

    @Override
    public void commitRequest(TransactionCommitRequest request, Listener<TransactionCommitResponse> listener) {
        transportClient.send(request, COMMIT_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request) throws IOException {
        return transportClient.send(request, DELETE_RESPONSE_MAPPER);
    }

    @Override
    public void deleteRequest(TransactionDeleteRequest request, Listener<TransactionDeleteResponse> listener) {
        transportClient.send(request, DELETE_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<TransactionPopResponse> popRequest(TransactionPopRequest request) throws IOException {
        return transportClient.send(request, POP_RESPONSE_MAPPER);
    }

    @Override
    public void popRequest(TransactionPopRequest request, Listener<TransactionPopResponse> listener) {
        transportClient.send(request, POP_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request) throws IOException {
        return transportClient.send(request, PUSH_RESPONSE_MAPPER);
    }

    @Override
    public void pushRequest(TransactionPushRequest request, Listener<TransactionPushResponse> listener) {
        transportClient.send(request, PUSH_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request) throws IOException {
        return transportClient.send(request, ROLLBACK_RESPONSE_MAPPER);
    }

    @Override
    public void rollbackRequest(TransactionRollbackRequest request, Listener<TransactionRollbackResponse> listener) {
        transportClient.send(request, ROLLBACK_RESPONSE_MAPPER).on(listener);
    }
}
