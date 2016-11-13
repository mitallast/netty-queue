package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;

import java.io.IOException;

public class ChangeConfiguration implements Streamable {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChangeConfiguration that = (ChangeConfiguration) o;

        return newConf.equals(that.newConf);
    }

    @Override
    public int hashCode() {
        return newConf.hashCode();
    }
}
