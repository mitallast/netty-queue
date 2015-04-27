package org.mitallast.queue.client.base;

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
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public interface QueueTransactionalClient {

    SmartFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request) throws IOException;

    void commitRequest(TransactionCommitRequest request, Listener<TransactionCommitResponse> listener);

    SmartFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request) throws IOException;

    void deleteRequest(TransactionDeleteRequest request, Listener<TransactionDeleteResponse> listener);

    SmartFuture<TransactionPopResponse> popRequest(TransactionPopRequest request) throws IOException;

    void popRequest(TransactionPopRequest request, Listener<TransactionPopResponse> listener);

    SmartFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request) throws IOException;

    void pushRequest(TransactionPushRequest request, Listener<TransactionPushResponse> listener);

    SmartFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request) throws IOException;

    void rollbackRequest(TransactionRollbackRequest request, Listener<TransactionRollbackResponse> listener);
}
