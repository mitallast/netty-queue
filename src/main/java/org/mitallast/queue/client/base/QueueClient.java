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
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public interface QueueClient {

    SmartFuture<EnQueueResponse> enqueueRequest(EnQueueRequest request) throws IOException;

    SmartFuture<DeQueueResponse> dequeueRequest(DeQueueRequest request) throws IOException;

    SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) throws IOException;

    SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) throws IOException;

    SmartFuture<GetResponse> getRequest(GetRequest request) throws IOException;

    SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) throws IOException;
}
