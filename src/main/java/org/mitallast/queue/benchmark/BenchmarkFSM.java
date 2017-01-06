package org.mitallast.queue.benchmark;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.resource.ResourceFSM;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.raft.resource.ResourceRegistry;

import java.util.Optional;

public class BenchmarkFSM extends AbstractComponent implements ResourceFSM {

    @Inject
    public BenchmarkFSM(Config config, ResourceRegistry registry) {
        super(config, BenchmarkFSM.class);
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
