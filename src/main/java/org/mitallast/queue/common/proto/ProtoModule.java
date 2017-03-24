package org.mitallast.queue.common.proto;

import com.google.inject.AbstractModule;

public class ProtoModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ProtoService.class).asEagerSingleton();
    }
}
