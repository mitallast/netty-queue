package org.mitallast.queue.client.base;

import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public interface QueueClient {

    SmartFuture<EnQueueResponse> enqueueRequest(EnQueueRequest request) throws IOException;

    void enqueueRequest(EnQueueRequest request, Listener<EnQueueResponse> listener) throws IOException;

    SmartFuture<DeQueueResponse> dequeueRequest(DeQueueRequest request) throws IOException;

    void dequeueRequest(DeQueueRequest request, Listener<DeQueueResponse> listener) throws IOException;

    SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) throws IOException;

    void queueStatsRequest(QueueStatsRequest request, Listener<QueueStatsResponse> listener) throws IOException;

    SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) throws IOException;

    void deleteRequest(DeleteRequest request, Listener<DeleteResponse> listener) throws IOException;

    SmartFuture<GetResponse> getRequest(GetRequest request) throws IOException;

    void getRequest(GetRequest request, Listener<GetResponse> listener) throws IOException;

    SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) throws IOException;

    void peekQueueRequest(PeekQueueRequest request, Listener<PeekQueueResponse> listener) throws IOException;
}
