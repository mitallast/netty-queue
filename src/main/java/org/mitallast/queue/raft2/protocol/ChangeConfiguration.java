package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.cluster.ClusterConfiguration;

import java.io.IOException;

public class ChangeConfiguration implements RaftMessage {
    private ClusterConfiguration newConf;

    protected ChangeConfiguration() {
    }

    public ChangeConfiguration(ClusterConfiguration newConf) {
        this.newConf = newConf;
    }

    public ClusterConfiguration getNewConf() {
        return newConf;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        newConf = stream.readStreamable();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeClass(newConf.getClass());
        stream.writeStreamable(newConf);
    }
}
