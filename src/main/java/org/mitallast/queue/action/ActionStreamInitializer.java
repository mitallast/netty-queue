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

        streamService.registerClass(DeleteRequest.Builder.class, DeleteRequest.Builder::new, ++id);
        streamService.registerClass(DeleteResponse.Builder.class, DeleteResponse.Builder::new, ++id);

        streamService.registerClass(GetRequest.Builder.class, GetRequest.Builder::new, ++id);
        streamService.registerClass(GetResponse.Builder.class, GetResponse.Builder::new, ++id);

        streamService.registerClass(PeekQueueResponse.Builder.class, PeekQueueResponse.Builder::new, ++id);
        streamService.registerClass(PeekQueueRequest.Builder.class, PeekQueueRequest.Builder::new, ++id);

        streamService.registerClass(PopRequest.Builder.class, PopRequest.Builder::new, ++id);
        streamService.registerClass(PopResponse.Builder.class, PopResponse.Builder::new, ++id);

        streamService.registerClass(PushRequest.Builder.class, PushRequest.Builder::new, ++id);
        streamService.registerClass(PushResponse.Builder.class, PushResponse.Builder::new, ++id);

        streamService.registerClass(QueueStatsRequest.Builder.class, QueueStatsRequest.Builder::new, ++id);
        streamService.registerClass(QueueStatsResponse.Builder.class, QueueStatsResponse.Builder::new, ++id);

        // transactional

        streamService.registerClass(TransactionCommitRequest.Builder.class, TransactionCommitRequest.Builder::new, ++id);
        streamService.registerClass(TransactionCommitResponse.Builder.class, TransactionCommitResponse.Builder::new, ++id);

        streamService.registerClass(TransactionDeleteRequest.Builder.class, TransactionDeleteRequest.Builder::new, ++id);
        streamService.registerClass(TransactionDeleteResponse.Builder.class, TransactionDeleteResponse.Builder::new, ++id);

        streamService.registerClass(TransactionPopRequest.Builder.class, TransactionPopRequest.Builder::new, ++id);
        streamService.registerClass(TransactionPopResponse.Builder.class, TransactionPopResponse.Builder::new, ++id);

        streamService.registerClass(TransactionPushRequest.Builder.class, TransactionPushRequest.Builder::new, ++id);
        streamService.registerClass(TransactionPushResponse.Builder.class, TransactionPushResponse.Builder::new, ++id);

        streamService.registerClass(TransactionRollbackRequest.Builder.class, TransactionRollbackRequest.Builder::new, ++id);
        streamService.registerClass(TransactionRollbackResponse.Builder.class, TransactionRollbackResponse.Builder::new, ++id);

        // queues

        streamService.registerClass(CreateQueueRequest.Builder.class, CreateQueueRequest.Builder::new, ++id);
        streamService.registerClass(CreateQueueResponse.Builder.class, CreateQueueResponse.Builder::new, ++id);

        streamService.registerClass(DeleteQueueRequest.Builder.class, DeleteQueueRequest.Builder::new, ++id);
        streamService.registerClass(DeleteQueueResponse.Builder.class, DeleteQueueResponse.Builder::new, ++id);

        streamService.registerClass(QueuesStatsRequest.Builder.class, QueuesStatsRequest.Builder::new, ++id);
        streamService.registerClass(QueuesStatsResponse.Builder.class, QueuesStatsResponse.Builder::new, ++id);
    }
}
