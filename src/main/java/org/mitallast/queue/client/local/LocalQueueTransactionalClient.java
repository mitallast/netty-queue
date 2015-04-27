package org.mitallast.queue.client.local;

import com.google.inject.Inject;
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
import org.mitallast.queue.client.base.QueueTransactionalClient;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public class LocalQueueTransactionalClient implements QueueTransactionalClient {

    private final TransactionCommitAction commitAction;
    private final TransactionDeleteAction deleteAction;
    private final TransactionPopAction popAction;
    private final TransactionPushAction pushAction;
    private final TransactionRollbackAction rollbackAction;

    @Inject
    public LocalQueueTransactionalClient(
        TransactionCommitAction commitAction,
        TransactionDeleteAction deleteAction,
        TransactionPopAction popAction,
        TransactionPushAction pushAction,
        TransactionRollbackAction rollbackAction) {
        this.commitAction = commitAction;
        this.deleteAction = deleteAction;
        this.popAction = popAction;
        this.pushAction = pushAction;
        this.rollbackAction = rollbackAction;
    }

    @Override
    public SmartFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request) throws IOException {
        return commitAction.execute(request);
    }

    @Override
    public void enqueueRequest(TransactionCommitRequest request, Listener<TransactionCommitResponse> listener) {
        commitAction.execute(request, listener);
    }

    @Override
    public SmartFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request) throws IOException {
        return deleteAction.execute(request);
    }

    @Override
    public void deleteRequest(TransactionDeleteRequest request, Listener<TransactionDeleteResponse> listener) {
        deleteAction.execute(request, listener);
    }

    @Override
    public SmartFuture<TransactionPopResponse> popRequest(TransactionPopRequest request) throws IOException {
        return popAction.execute(request);
    }

    @Override
    public void popRequest(TransactionPopRequest request, Listener<TransactionPopResponse> listener) {
        popAction.execute(request, listener);
    }

    @Override
    public SmartFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request) throws IOException {
        return pushAction.execute(request);
    }

    @Override
    public void pushRequest(TransactionPushRequest request, Listener<TransactionPushResponse> listener) {
        pushAction.execute(request, listener);
    }

    @Override
    public SmartFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request) throws IOException {
        return rollbackAction.execute(request);
    }

    @Override
    public void rollbackRequest(TransactionRollbackRequest request, Listener<TransactionRollbackResponse> listener) {
        rollbackAction.execute(request, listener);
    }
}
