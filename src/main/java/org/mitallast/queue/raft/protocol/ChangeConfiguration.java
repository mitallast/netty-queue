package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft.RaftMessage;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;

import java.io.IOException;

public class ChangeConfiguration implements RaftMessage {
    private final ClusterConfiguration newConf;

    public ChangeConfiguration(StreamInput stream) throws IOException {
        newConf = stream.readStreamable();
    }

    public ChangeConfiguration(ClusterConfiguration newConf) {
        this.newConf = newConf;
    }

    public ClusterConfiguration getNewConf() {
        return newConf;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeClass(newConf.getClass());
        stream.writeStreamable(newConf);
    }
}
