package org.mitallast.queue.transport;

import com.google.inject.AbstractModule;
import org.mitallast.queue.transport.http.HttpServer;
import org.mitallast.queue.transport.http.HttpServerHandler;
import org.mitallast.queue.transport.http.HttpServerInitializer;

public class TransportModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServerHandler.class).asEagerSingleton();
        bind(HttpServerInitializer.class).asEagerSingleton();
        bind(HttpServer.class).asEagerSingleton();
    }
}
