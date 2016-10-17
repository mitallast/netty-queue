package org.mitallast.queue.rest;

import com.google.inject.AbstractModule;
import org.mitallast.queue.rest.action.ResourceAction;
import org.mitallast.queue.rest.action.RestIndexAction;
import org.mitallast.queue.rest.transport.HttpServer;

public class RestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServer.class).asEagerSingleton();
        bind(RestController.class).asEagerSingleton();
        bind(RestIndexAction.class).asEagerSingleton();
        bind(ResourceAction.class).asEagerSingleton();
    }
}
