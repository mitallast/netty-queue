package org.mitallast.queue.transport;

import com.google.inject.AbstractModule;
import org.mitallast.queue.transport.netty.NettyTransportService;
import org.mitallast.queue.transport.netty.TransportServer;

public class TransportModule extends AbstractModule {
    @Override
    protected void configure() {
        // implementation instance
        bind(TransportServer.class).asEagerSingleton();
        bind(TransportController.class).asEagerSingleton();
        bind(NettyTransportService.class).asEagerSingleton();

        // interface inject
        bind(TransportService.class).to(NettyTransportService.class);
    }
}
