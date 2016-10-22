package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class RaftMembersDiscoveryRequest implements Streamable {

    public final static RaftMembersDiscoveryRequest INSTANCE = new RaftMembersDiscoveryRequest();

    public static RaftMembersDiscoveryRequest read(StreamInput input) throws IOException {
        return INSTANCE;
    }

    protected RaftMembersDiscoveryRequest() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
