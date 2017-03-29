package org.mitallast.queue.common.component;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;

public class ComponentModule extends AbstractModule {

    private final Config config;

    public ComponentModule(Config config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        LifecycleService lifecycleService = new LifecycleService();
        bind(Config.class).toInstance(config);
        bind(LifecycleService.class).toInstance(lifecycleService);
        bindListener(new LifecycleMatcher(), lifecycleService);
    }
}

