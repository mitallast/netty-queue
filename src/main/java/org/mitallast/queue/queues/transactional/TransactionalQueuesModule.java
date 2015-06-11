package org.mitallast.queue.queues.transactional;

import com.google.inject.AbstractModule;

public class TransactionalQueuesModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TransactionalQueuesService.class).to(InternalTransactionalQueuesService.class);
        bind(InternalTransactionalQueuesService.class).asEagerSingleton();
    }
}
