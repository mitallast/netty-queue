package org.mitallast.queue.raft;

import com.google.inject.Inject;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.state.RaftStateClient;

import java.util.concurrent.CompletableFuture;

public class RaftClient implements Protocol {

    private final RaftStateClient client;

    @Inject
    private RaftClient(RaftStateClient client) {
        this.client = client;
    }

    @Override
    public <T extends Streamable> CompletableFuture<T> submit(Command<T> command) {
        return client.submit(command);
    }

    @Override
    public <T extends Streamable> CompletableFuture<T> submit(Query<T> query) {
        return client.submit(query);
    }

    @Override
    public void delete() {
    }
}
