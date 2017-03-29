package org.mitallast.queue.raft.rest;

import com.google.inject.AbstractModule;

public class RaftRestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RaftHandler.class).asEagerSingleton();
    }
}
