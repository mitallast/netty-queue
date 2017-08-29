package org.mitallast.queue.transport;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.ecdh.Encrypted;
import org.mitallast.queue.ecdh.RequestStart;
import org.mitallast.queue.ecdh.ResponseStart;
import org.mitallast.queue.transport.netty.NettyTransportServer;
import org.mitallast.queue.transport.netty.NettyTransportService;

public class TransportModule extends AbstractModule {

    static {
        Codec.register(10, RequestStart.class, RequestStart.codec);
        Codec.register(11, ResponseStart.class, ResponseStart.codec);
        Codec.register(12, Encrypted.class, Encrypted.codec);
    }

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
