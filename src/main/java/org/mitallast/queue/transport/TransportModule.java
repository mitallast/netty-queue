package org.mitallast.queue.transport;

import com.google.inject.AbstractModule;
import org.mitallast.queue.transport.transport.TransportServer;

public class TransportModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TransportServer.class).asEagerSingleton();
        bind(TransportController.class).asEagerSingleton();
    }
}
