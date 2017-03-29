package org.mitallast.queue.benchmark;

import com.google.inject.Inject;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import java.util.Optional;

public class BenchmarkFSM implements ResourceFSM {

    @Inject
    public BenchmarkFSM(ResourceRegistry registry) {
        registry.register(this);
        registry.register(BenchmarkRequest.class, this::handle);
    }

    public Streamable handle(BenchmarkRequest request) {
        return new BenchmarkResponse(request.getRequest());
    }

    @Override
    public Optional<Streamable> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.empty();
    }
}
