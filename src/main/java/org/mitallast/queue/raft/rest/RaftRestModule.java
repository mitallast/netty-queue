package org.mitallast.queue.raft.rest;

import com.google.inject.AbstractModule;

public class RaftRestModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RaftStateAction.class).asEagerSingleton();
        bind(RaftLogAction.class).asEagerSingleton();
    }
}
