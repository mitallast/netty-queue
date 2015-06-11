package org.mitallast.queue.common.component;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.settings.Settings;

public class ComponentModule extends AbstractModule {

    private final Settings settings;

    public ComponentModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        LifecycleService lifecycleService = new LifecycleService(settings);
        bind(Settings.class).toInstance(settings);
        bind(LifecycleService.class).toInstance(lifecycleService);
        bindListener(new LifecycleMatcher(), lifecycleService);
    }
}

