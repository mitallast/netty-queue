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
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.common.stream.StreamService;

public class ActionStreamInitializer {
    @Inject
    public ActionStreamInitializer(StreamService streamService) {
        int id = 10000;
        streamService.registerClass(DeleteRequest.class, ++id);
        streamService.registerClass(DeleteResponse.class, ++id);

        streamService.registerClass(GetRequest.class, ++id);
        streamService.registerClass(GetResponse.class, ++id);

        streamService.registerClass(PeekQueueResponse.class, ++id);
        streamService.registerClass(PeekQueueRequest.class, ++id);

        streamService.registerClass(PopRequest.class, ++id);
        streamService.registerClass(PopResponse.class, ++id);

        streamService.registerClass(PushRequest.class, ++id);
        streamService.registerClass(PushResponse.class, ++id);

        streamService.registerClass(QueuesStatsRequest.class, ++id);
        streamService.registerClass(QueuesStatsResponse.class, ++id);

        streamService.registerClass(TransactionCommitRequest.class, ++id);
        streamService.registerClass(TransactionCommitResponse.class, ++id);

        streamService.registerClass(TransactionDeleteRequest.class, ++id);
        streamService.registerClass(TransactionDeleteResponse.class, ++id);

        streamService.registerClass(TransactionPopRequest.class, ++id);
        streamService.registerClass(TransactionPopResponse.class, ++id);

        streamService.registerClass(TransactionPushRequest.class, ++id);
        streamService.registerClass(TransactionPushResponse.class, ++id);

        streamService.registerClass(TransactionRollbackRequest.class, ++id);
        streamService.registerClass(TransactionRollbackResponse.class, ++id);
    }
}
