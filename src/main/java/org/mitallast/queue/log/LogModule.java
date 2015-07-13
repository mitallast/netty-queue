package org.mitallast.queue.log;

import com.google.inject.AbstractModule;

public class LogModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LogStreamService.class).asEagerSingleton();
        bind(LogService.class).asEagerSingleton();
    }
}