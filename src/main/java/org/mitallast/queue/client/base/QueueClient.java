package org.mitallast.queue.client.base;

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

import java.io.IOException;

public interface QueueClient {

    QueueTransactionalClient transactional();

    SmartFuture<PushResponse> pushRequest(PushRequest request) throws IOException;

    void pushRequest(PushRequest request, Listener<PushResponse> listener);

    SmartFuture<PopResponse> popRequest(PopRequest request) throws IOException;

    void popRequest(PopRequest request, Listener<PopResponse> listener);

    SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) throws IOException;

    void queueStatsRequest(QueueStatsRequest request, Listener<QueueStatsResponse> listener);

    SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) throws IOException;

    void deleteRequest(DeleteRequest request, Listener<DeleteResponse> listener);

    SmartFuture<GetResponse> getRequest(GetRequest request) throws IOException;

    void getRequest(GetRequest request, Listener<GetResponse> listener);

    SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) throws IOException;

    void peekQueueRequest(PeekQueueRequest request, Listener<PeekQueueResponse> listener);
}
