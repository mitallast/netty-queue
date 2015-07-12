package org.mitallast.queue.raft.state;

import com.google.inject.Inject;
import com.google.inject.Injector;

public class RaftStateFactory {
    private final Injector injector;

    @Inject
    public RaftStateFactory(Injector injector) {
        this.injector = injector;
    }

    public <T extends AbstractState> T create(Class<T> stateClass) {
        return injector.getInstance(stateClass);
    }
}
