package org.mitallast.queue.benchmark;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.ResourceFSM;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

import java.util.Optional;

public class BenchmarkFSM extends AbstractComponent implements ResourceFSM {

    @Inject
    public BenchmarkFSM(Config config) {
        super(config, BenchmarkFSM.class);
    }

    @Override
    public Streamable apply(Streamable message) {
        if(message instanceof BenchmarkRequest) {
            BenchmarkRequest request = (BenchmarkRequest) message;
            request.getData().release();
            return new BenchmarkResponse(request.getRequest());
        }
        return null;
    }

    @Override
    public Optional<RaftSnapshot> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return Optional.empty();
    }
}
