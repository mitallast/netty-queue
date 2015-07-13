package org.mitallast.queue.rest;

import com.google.inject.AbstractModule;
import org.mitallast.queue.rest.action.ResourceAction;
import org.mitallast.queue.rest.action.RestIndexAction;
import org.mitallast.queue.rest.action.queue.*;
import org.mitallast.queue.rest.action.queue.transactional.*;
import org.mitallast.queue.rest.action.queues.RestCreateQueueAction;
import org.mitallast.queue.rest.action.queues.RestDeleteQueueAction;
import org.mitallast.queue.rest.action.queues.RestQueuesStatsAction;
import org.mitallast.queue.rest.action.raft.RestCreateResourceAction;
import org.mitallast.queue.rest.action.raft.RestGetResourcesAction;
import org.mitallast.queue.rest.transport.HttpServer;

public class RestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServer.class).asEagerSingleton();

        bind(RestController.class).asEagerSingleton();

        bind(RestPopAction.class).asEagerSingleton();
        bind(RestPushAction.class).asEagerSingleton();
        bind(RestPeekQueueAction.class).asEagerSingleton();
        bind(RestDeleteAction.class).asEagerSingleton();
        bind(RestGetAction.class).asEagerSingleton();

        bind(RestTransactionCommitAction.class).asEagerSingleton();
        bind(RestTransactionDeleteAction.class).asEagerSingleton();
        bind(RestTransactionPushAction.class).asEagerSingleton();
        bind(RestTransactionPopAction.class).asEagerSingleton();
        bind(RestTransactionRollbackAction.class).asEagerSingleton();

        bind(RestQueueStatsAction.class).asEagerSingleton();

        bind(RestCreateQueueAction.class).asEagerSingleton();
        bind(RestDeleteQueueAction.class).asEagerSingleton();
        bind(RestQueuesStatsAction.class).asEagerSingleton();

        bind(RestGetResourcesAction.class).asEagerSingleton();
        bind(RestCreateResourceAction.class).asEagerSingleton();

        bind(RestIndexAction.class).asEagerSingleton();
        bind(ResourceAction.class).asEagerSingleton();
    }
}
