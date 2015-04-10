package org.mitallast.queue.client.local;

import com.google.inject.Inject;
import org.mitallast.queue.action.queue.delete.DeleteAction;
import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.action.queue.dequeue.DeQueueAction;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.action.queue.enqueue.EnQueueAction;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.action.queue.get.GetAction;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.action.queue.peek.PeekQueueAction;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsAction;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.client.base.QueueClient;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public class LocalQueueClient implements QueueClient {

    private final EnQueueAction enQueueAction;
    private final DeQueueAction deQueueAction;
    private final PeekQueueAction peekQueueAction;
    private final DeleteAction deleteAction;
    private final GetAction getAction;
    private final QueueStatsAction queueStatsAction;

    @Inject
    public LocalQueueClient(EnQueueAction enQueueAction,
                            DeQueueAction deQueueAction,
                            PeekQueueAction peekQueueAction,
                            DeleteAction deleteAction,
                            GetAction getAction,
                            QueueStatsAction queueStatsAction) {
        this.enQueueAction = enQueueAction;
        this.deQueueAction = deQueueAction;
        this.peekQueueAction = peekQueueAction;
        this.deleteAction = deleteAction;
        this.getAction = getAction;
        this.queueStatsAction = queueStatsAction;
    }

    @Override
    public SmartFuture<EnQueueResponse> enqueueRequest(EnQueueRequest request) throws IOException {
        return enQueueAction.execute(request);
    }

    @Override
    public void enqueueRequest(EnQueueRequest request, Listener<EnQueueResponse> listener) throws IOException {
        enQueueAction.execute(request, listener);
    }

    @Override
    public SmartFuture<DeQueueResponse> dequeueRequest(DeQueueRequest request) throws IOException {
        return deQueueAction.execute(request);
    }

    @Override
    public void dequeueRequest(DeQueueRequest request, Listener<DeQueueResponse> listener) throws IOException {
        deQueueAction.execute(request, listener);
    }

    @Override
    public SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) throws IOException {
        return queueStatsAction.execute(request);
    }

    @Override
    public void queueStatsRequest(QueueStatsRequest request, Listener<QueueStatsResponse> listener) throws IOException {
        queueStatsAction.execute(request, listener);
    }

    @Override
    public SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) throws IOException {
        return deleteAction.execute(request);
    }

    @Override
    public void deleteRequest(DeleteRequest request, Listener<DeleteResponse> listener) throws IOException {
        deleteAction.execute(request, listener);
    }

    @Override
    public SmartFuture<GetResponse> getRequest(GetRequest request) throws IOException {
        return getAction.execute(request);
    }

    @Override
    public void getRequest(GetRequest request, Listener<GetResponse> listener) throws IOException {
        getAction.execute(request, listener);
    }

    @Override
    public SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) throws IOException {
        return peekQueueAction.execute(request);
    }

    @Override
    public void peekQueueRequest(PeekQueueRequest request, Listener<PeekQueueResponse> listener) throws IOException {
        peekQueueAction.execute(request, listener);
    }
}
