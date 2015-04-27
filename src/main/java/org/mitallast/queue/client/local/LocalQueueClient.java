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
import org.mitallast.queue.client.base.QueueClient;
import org.mitallast.queue.client.base.QueueTransactionalClient;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

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
    public SmartFuture<PushResponse> pushRequest(PushRequest request) throws IOException {
        return pushAction.execute(request);
    }

    @Override
    public void pushRequest(PushRequest request, Listener<PushResponse> listener) {
        pushAction.execute(request, listener);
    }

    @Override
    public SmartFuture<PopResponse> popRequest(PopRequest request) throws IOException {
        return popAction.execute(request);
    }

    @Override
    public void popRequest(PopRequest request, Listener<PopResponse> listener) {
        popAction.execute(request, listener);
    }

    @Override
    public SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) throws IOException {
        return queueStatsAction.execute(request);
    }

    @Override
    public void queueStatsRequest(QueueStatsRequest request, Listener<QueueStatsResponse> listener) {
        queueStatsAction.execute(request, listener);
    }

    @Override
    public SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) throws IOException {
        return deleteAction.execute(request);
    }

    @Override
    public void deleteRequest(DeleteRequest request, Listener<DeleteResponse> listener) {
        deleteAction.execute(request, listener);
    }

    @Override
    public SmartFuture<GetResponse> getRequest(GetRequest request) throws IOException {
        return getAction.execute(request);
    }

    @Override
    public void getRequest(GetRequest request, Listener<GetResponse> listener) {
        getAction.execute(request, listener);
    }

    @Override
    public SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) throws IOException {
        return peekQueueAction.execute(request);
    }

    @Override
    public void peekQueueRequest(PeekQueueRequest request, Listener<PeekQueueResponse> listener) {
        peekQueueAction.execute(request, listener);
    }
}
