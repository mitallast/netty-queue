package org.mitallast.queue.common.json;

import com.google.inject.AbstractModule;

public class JsonModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JsonService.class).asEagerSingleton();
    }
}
