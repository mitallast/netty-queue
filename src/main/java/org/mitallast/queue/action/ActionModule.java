package org.mitallast.queue.action;

import com.google.inject.AbstractModule;
import org.mitallast.queue.action.queue.delete.DeleteAction;
import org.mitallast.queue.action.queue.dequeue.DeQueueAction;
import org.mitallast.queue.action.queue.enqueue.EnQueueAction;
import org.mitallast.queue.action.queue.get.GetAction;
import org.mitallast.queue.action.queue.peek.PeekQueueAction;
import org.mitallast.queue.action.queue.stats.QueueStatsAction;
import org.mitallast.queue.action.queue.transactional.commit.TransactionCommitAction;
import org.mitallast.queue.action.queue.transactional.delete.TransactionDeleteAction;
import org.mitallast.queue.action.queue.transactional.rollback.TransactionRollbackAction;
import org.mitallast.queue.action.queues.create.CreateQueueAction;
import org.mitallast.queue.action.queues.delete.DeleteQueueAction;
import org.mitallast.queue.action.queues.stats.QueuesStatsAction;

public class ActionModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EnQueueAction.class).asEagerSingleton();
        bind(DeQueueAction.class).asEagerSingleton();
        bind(PeekQueueAction.class).asEagerSingleton();
        bind(DeleteAction.class).asEagerSingleton();
        bind(GetAction.class).asEagerSingleton();

        bind(TransactionCommitAction.class).asEagerSingleton();
        bind(TransactionRollbackAction.class).asEagerSingleton();
        bind(TransactionDeleteAction.class).asEagerSingleton();

        bind(QueueStatsAction.class).asEagerSingleton();

        bind(CreateQueueAction.class).asEagerSingleton();
        bind(DeleteQueueAction.class).asEagerSingleton();
        bind(QueuesStatsAction.class).asEagerSingleton();
    }
}
