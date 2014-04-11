package org.mitallast.queue.queues;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.settings.Settings;

public class QueuesModule extends AbstractModule {

    private final Settings settings;

    public QueuesModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(Settings.class).toInstance(settings);
        bind(QueuesService.class).to(InternalQueuesService.class).asEagerSingleton();
    }
}
