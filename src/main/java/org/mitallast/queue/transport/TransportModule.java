package org.mitallast.queue.transport;

import com.google.inject.AbstractModule;
import org.mitallast.queue.transport.netty.NettyTransportServer;
import org.mitallast.queue.transport.netty.NettyTransportService;

public class TransportModule extends AbstractModule {
    @Override
    protected void configure() {
        // implementation instance
        bind(NettyTransportServer.class).asEagerSingleton();
        bind(TransportController.class).asEagerSingleton();
        bind(NettyTransportService.class).asEagerSingleton();

        // interface inject
        bind(TransportServer.class).to(NettyTransportServer.class);
        bind(TransportService.class).to(NettyTransportService.class);
    }
}
