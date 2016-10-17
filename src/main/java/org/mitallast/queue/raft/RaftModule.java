package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;

public class RaftModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Raft.class).asEagerSingleton();
        bind(RaftStreamService.class).asEagerSingleton();
        bind(RaftHandler.class).asEagerSingleton();
    }
}
