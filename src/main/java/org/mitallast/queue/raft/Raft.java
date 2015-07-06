package org.mitallast.queue.raft;

import com.google.inject.Inject;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.state.RaftStateContext;

import java.util.concurrent.CompletableFuture;

public class Raft implements Protocol {

    private final RaftStateContext context;

    @Inject
    public Raft(RaftStateContext context) {
        this.context = context;
    }

    public long term() {
        return context.getTerm();
    }

    public State state() {
        return context.getState();
    }

    @Override
    public <T extends Streamable> CompletableFuture<T> submit(Command<T> command) {
        return context.submit(command);
    }

    @Override
    public <T extends Streamable> CompletableFuture<T> submit(Query<T> query) {
        return context.submit(query);
    }

    @Override
    public void delete() {
        context.delete();
    }

    public enum State {START, PASSIVE, FOLLOWER, CANDIDATE, LEADER}
}
