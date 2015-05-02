package org.mitallast.queue.client;

import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.action.queue.pop.PopRequest;
import org.mitallast.queue.action.queue.pop.PopResponse;
import org.mitallast.queue.action.queue.push.PushRequest;
import org.mitallast.queue.action.queue.push.PushResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

public interface QueueClient {

    QueueTransactionalClient transactional();

    SmartFuture<PushResponse> pushRequest(PushRequest request);

    default void pushRequest(PushRequest request, Listener<PushResponse> listener) {
        pushRequest(request).on(listener);
    }

    SmartFuture<PopResponse> popRequest(PopRequest request);

    default void popRequest(PopRequest request, Listener<PopResponse> listener) {
        popRequest(request).on(listener);
    }

    SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request);

    default void queueStatsRequest(QueueStatsRequest request, Listener<QueueStatsResponse> listener) {
        queueStatsRequest(request).on(listener);
    }

    SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request);

    default void deleteRequest(DeleteRequest request, Listener<DeleteResponse> listener) {
        deleteRequest(request).on(listener);
    }

    SmartFuture<GetResponse> getRequest(GetRequest request);

    default void getRequest(GetRequest request, Listener<GetResponse> listener) {
        getRequest(request).on(listener);
    }

    SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request);

    default void peekQueueRequest(PeekQueueRequest request, Listener<PeekQueueResponse> listener) {
        peekQueueRequest(request).on(listener);
    }
}
