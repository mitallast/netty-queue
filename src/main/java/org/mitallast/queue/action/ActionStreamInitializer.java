package org.mitallast.queue.action;

import com.google.inject.Inject;
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
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.stream.StreamService;

public class ActionStreamInitializer {
    @Inject
    public ActionStreamInitializer(StreamService streamService) {
        int id = 1000;

        // queue

        streamService.registerClass(DeleteRequest.Builder.class, ++id);
        streamService.registerClass(DeleteResponse.Builder.class, ++id);

        streamService.registerClass(GetRequest.Builder.class, ++id);
        streamService.registerClass(GetResponse.Builder.class, ++id);

        streamService.registerClass(PeekQueueResponse.Builder.class, ++id);
        streamService.registerClass(PeekQueueRequest.Builder.class, ++id);

        streamService.registerClass(PopRequest.Builder.class, ++id);
        streamService.registerClass(PopResponse.Builder.class, ++id);

        streamService.registerClass(PushRequest.Builder.class, ++id);
        streamService.registerClass(PushResponse.Builder.class, ++id);

        streamService.registerClass(QueueStatsRequest.Builder.class, ++id);
        streamService.registerClass(QueueStatsResponse.Builder.class, ++id);

        // transactional

        streamService.registerClass(TransactionCommitRequest.Builder.class, ++id);
        streamService.registerClass(TransactionCommitResponse.Builder.class, ++id);

        streamService.registerClass(TransactionDeleteRequest.Builder.class, ++id);
        streamService.registerClass(TransactionDeleteResponse.Builder.class, ++id);

        streamService.registerClass(TransactionPopRequest.Builder.class, ++id);
        streamService.registerClass(TransactionPopResponse.Builder.class, ++id);

        streamService.registerClass(TransactionPushRequest.Builder.class, ++id);
        streamService.registerClass(TransactionPushResponse.Builder.class, ++id);

        streamService.registerClass(TransactionRollbackRequest.Builder.class, ++id);
        streamService.registerClass(TransactionRollbackResponse.Builder.class, ++id);

        // queues

        streamService.registerClass(CreateQueueRequest.Builder.class, ++id);
        streamService.registerClass(CreateQueueResponse.Builder.class, ++id);

        streamService.registerClass(DeleteQueueRequest.Builder.class, ++id);
        streamService.registerClass(DeleteQueueResponse.Builder.class, ++id);

        streamService.registerClass(QueuesStatsRequest.Builder.class, ++id);
        streamService.registerClass(QueuesStatsResponse.Builder.class, ++id);
    }
}
