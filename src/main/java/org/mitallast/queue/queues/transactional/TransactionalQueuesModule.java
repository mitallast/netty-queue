package org.mitallast.queue.queues.transactional;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.settings.Settings;

public class TransactionalQueuesModule extends AbstractModule {

    private final Settings settings;

    public TransactionalQueuesModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(Settings.class).toInstance(settings);
        bind(TransactionalQueuesService.class).to(InternalTransactionalQueuesService.class);
        bind(InternalTransactionalQueuesService.class).asEagerSingleton();
    }
}
