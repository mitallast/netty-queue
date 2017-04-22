package org.mitallast.queue.crdt.rest;

import com.google.inject.AbstractModule;

public class RestCrdtModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RestCrdtRouting.class).asEagerSingleton();
        bind(RestLWWRegister.class).asEagerSingleton();
        bind(RestGCounter.class).asEagerSingleton();
    }
}
