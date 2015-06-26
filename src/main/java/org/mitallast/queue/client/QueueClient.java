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

import java.util.concurrent.CompletableFuture;

public interface QueueClient {

    QueueTransactionalClient transactional();

    CompletableFuture<PushResponse> pushRequest(PushRequest request);

    CompletableFuture<PopResponse> popRequest(PopRequest request);

    CompletableFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request);

    CompletableFuture<DeleteResponse> deleteRequest(DeleteRequest request);

    CompletableFuture<GetResponse> getRequest(GetRequest request);

    CompletableFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request);
}
