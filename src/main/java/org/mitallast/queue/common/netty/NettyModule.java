package org.mitallast.queue.common.netty;

import com.google.inject.AbstractModule;

public class NettyModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(NettyProvider.class).asEagerSingleton();
    }
}
