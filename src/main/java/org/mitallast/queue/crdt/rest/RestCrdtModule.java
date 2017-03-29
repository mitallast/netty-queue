package org.mitallast.queue.crdt.rest;

import com.google.inject.AbstractModule;

public class RestCrdtModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RestCrdtHandler.class).asEagerSingleton();
    }
}
