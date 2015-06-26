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

import java.util.concurrent.CompletableFuture;

public interface QueueTransactionalClient {

    CompletableFuture<TransactionCommitResponse> commitRequest(TransactionCommitRequest request);

    CompletableFuture<TransactionDeleteResponse> deleteRequest(TransactionDeleteRequest request);

    CompletableFuture<TransactionPopResponse> popRequest(TransactionPopRequest request);

    CompletableFuture<TransactionPushResponse> pushRequest(TransactionPushRequest request);

    CompletableFuture<TransactionRollbackResponse> rollbackRequest(TransactionRollbackRequest request);
}
