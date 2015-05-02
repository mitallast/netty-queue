package org.mitallast.queue.client;

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

public interface QueueTransactionalClient {

    SmartFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request);

    default void commitRequest(TransactionCommitRequest request, Listener<TransactionCommitResponse> listener) {
        commitRequest(request).on(listener);
    }

    SmartFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request);

    default void deleteRequest(TransactionDeleteRequest request, Listener<TransactionDeleteResponse> listener) {
        deleteRequest(request).on(listener);
    }

    SmartFuture<TransactionPopResponse> popRequest(TransactionPopRequest request);

    default void popRequest(TransactionPopRequest request, Listener<TransactionPopResponse> listener) {
        popRequest(request).on(listener);
    }

    SmartFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request);

    default void pushRequest(TransactionPushRequest request, Listener<TransactionPushResponse> listener) {
        pushRequest(request).on(listener);
    }

    SmartFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request);

    default void rollbackRequest(TransactionRollbackRequest request, Listener<TransactionRollbackResponse> listener) {
        rollbackRequest(request).on(listener);
    }
}
