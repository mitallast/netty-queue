package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class RaftMembersDiscoveryTimeout implements Streamable {

    public final static RaftMembersDiscoveryTimeout INSTANCE = new RaftMembersDiscoveryTimeout();

    public static RaftMembersDiscoveryTimeout read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private RaftMembersDiscoveryTimeout() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
