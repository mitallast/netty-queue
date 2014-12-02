package org.mitallast.queue.client;

import com.google.inject.Inject;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.FutureActionListener;
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

public class QueueClient {
    private final EnQueueAction enQueueAction;
    private final DeQueueAction deQueueAction;
    private final PeekQueueAction peekQueueAction;
    private final DeleteAction deleteAction;
    private final GetAction getAction;
    private final QueueStatsAction queueStatsAction;

    @Inject
    public QueueClient(EnQueueAction enQueueAction,
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

    public FutureActionListener<EnQueueResponse> enqueueRequest(EnQueueRequest request) {
        return enQueueAction.execute(request);
    }

    public void enqueueRequest(EnQueueRequest request, ActionListener<EnQueueResponse> listener) {
        enQueueAction.execute(request, listener);
    }

    public FutureActionListener<DeQueueResponse> dequeueRequest(DeQueueRequest request) {
        return deQueueAction.execute(request);
    }

    public void dequeueRequest(DeQueueRequest request, ActionListener<DeQueueResponse> listener) {
        deQueueAction.execute(request, listener);
    }

    public FutureActionListener<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) {
        return queueStatsAction.execute(request);
    }

    public void queueStatsRequest(QueueStatsRequest request, ActionListener<QueueStatsResponse> listener) {
        queueStatsAction.execute(request, listener);
    }

    public FutureActionListener<DeleteResponse> deleteRequest(DeleteRequest request) {
        return deleteAction.execute(request);
    }

    public void deleteRequest(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        deleteAction.execute(request, listener);
    }

    public FutureActionListener<GetResponse> getRequest(GetRequest request) {
        return getAction.execute(request);
    }

    public void getRequest(GetRequest request, ActionListener<GetResponse> listener) {
        getAction.execute(request, listener);
    }

    public FutureActionListener<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) {
        return peekQueueAction.execute(request);
    }

    public void peekQueueRequest(PeekQueueRequest request, ActionListener<PeekQueueResponse> listener) {
        peekQueueAction.execute(request, listener);
    }
}
