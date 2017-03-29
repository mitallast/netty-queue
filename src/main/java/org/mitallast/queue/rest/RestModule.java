package org.mitallast.queue.rest;

import com.google.inject.AbstractModule;
import org.mitallast.queue.rest.action.ResourceHandler;
import org.mitallast.queue.rest.action.SettingsAction;
import org.mitallast.queue.rest.netty.HttpServer;
import org.mitallast.queue.rest.netty.HttpServerHandler;
import org.mitallast.queue.rest.netty.WebSocketFrameHandler;

public class RestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServer.class).asEagerSingleton();
        bind(HttpServerHandler.class).asEagerSingleton();
        bind(WebSocketFrameHandler.class).asEagerSingleton();
        bind(RestController.class).asEagerSingleton();

        bind(ResourceHandler.class).asEagerSingleton();
        bind(SettingsAction.class).asEagerSingleton();
    }
}
