package org.mitallast.queue.raft2.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;

import java.io.IOException;

public class RaftMembersDiscoveryRequest implements RaftMessage {

    public final static RaftMembersDiscoveryRequest INSTANCE = new RaftMembersDiscoveryRequest();

    protected RaftMembersDiscoveryRequest() {
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
