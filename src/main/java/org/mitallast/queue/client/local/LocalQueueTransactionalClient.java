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
import org.mitallast.queue.client.QueueTransactionalClient;

import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request) {
        return commitAction.execute(request);
    }

    @Override
    public CompletableFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request) {
        return deleteAction.execute(request);
    }

    @Override
    public CompletableFuture<TransactionPopResponse> popRequest(TransactionPopRequest request) {
        return popAction.execute(request);
    }

    @Override
    public CompletableFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request) {
        return pushAction.execute(request);
    }

    @Override
    public CompletableFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request) {
        return rollbackAction.execute(request);
    }
}
