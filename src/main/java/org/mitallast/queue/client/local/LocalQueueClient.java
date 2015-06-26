package org.mitallast.queue.client.local;

import com.google.inject.Inject;
import org.mitallast.queue.action.queue.delete.DeleteAction;
import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.action.queue.get.GetAction;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.action.queue.peek.PeekQueueAction;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.action.queue.pop.PopAction;
import org.mitallast.queue.action.queue.pop.PopRequest;
import org.mitallast.queue.action.queue.pop.PopResponse;
import org.mitallast.queue.action.queue.push.PushAction;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsAction;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.client.QueueTransactionalClient;

import java.util.concurrent.CompletableFuture;

public class LocalQueueClient implements QueueClient {

    private final PushAction pushAction;
    private final PopAction popAction;
    private final PeekQueueAction peekQueueAction;
    private final DeleteAction deleteAction;
    private final GetAction getAction;
    private final QueueStatsAction queueStatsAction;
    private final LocalQueueTransactionalClient queueTransactionalClient;

    @Inject
    public LocalQueueClient(
        PushAction pushAction,
        PopAction popAction,
        PeekQueueAction peekQueueAction,
        DeleteAction deleteAction,
        GetAction getAction,
        QueueStatsAction queueStatsAction,
        LocalQueueTransactionalClient queueTransactionalClient
    ) {
        this.pushAction = pushAction;
        this.popAction = popAction;
        this.peekQueueAction = peekQueueAction;
        this.deleteAction = deleteAction;
        this.getAction = getAction;
        this.queueStatsAction = queueStatsAction;
        this.queueTransactionalClient = queueTransactionalClient;
    }

    @Override
    public QueueTransactionalClient transactional() {
        return queueTransactionalClient;
    }

    @Override
    public CompletableFuture<PushResponse> pushRequest(PushRequest request) {
        return pushAction.execute(request);
    }

    @Override
    public CompletableFuture<PopResponse> popRequest(PopRequest request) {
        return popAction.execute(request);
    }

    @Override
    public CompletableFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) {
        return queueStatsAction.execute(request);
    }

    @Override
    public CompletableFuture<DeleteResponse> deleteRequest(DeleteRequest request) {
        return deleteAction.execute(request);
    }

    @Override
    public CompletableFuture<GetResponse> getRequest(GetRequest request) {
        return getAction.execute(request);
    }

    @Override
    public CompletableFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) {
        return peekQueueAction.execute(request);
    }
}
