package org.mitallast.queue.common.events;

import com.google.inject.AbstractModule;

public class EventBusModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DefaultEventBus.class).asEagerSingleton();
        bind(EventBus.class).to(DefaultEventBus.class);
    }
}
