package org.mitallast.queue.raft.cluster;

import javaslang.collection.Set;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

public interface ClusterConfiguration extends Message {
    Codec<ClusterConfiguration> codec = new ClusterConfigurationCodec();

    Set<DiscoveryNode> members();

    boolean isTransitioning();

    ClusterConfiguration transitionTo(ClusterConfiguration state);

    ClusterConfiguration transitionToStable();

    boolean containsOnNewState(DiscoveryNode member);
}


class ClusterConfigurationCodec implements Codec<ClusterConfiguration> {
    @Override
    public ClusterConfiguration read(DataInput stream) {
        try {
            boolean isTransitioning = stream.readBoolean();
            if (isTransitioning) {
                return JointConsensusClusterConfiguration.codec.read(stream);
            } else {
                return StableClusterConfiguration.codec.read(stream);
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, ClusterConfiguration value) {
        try {
            boolean isTransitioning = value.isTransitioning();
            stream.writeBoolean(isTransitioning);
            if (isTransitioning) {
                JointConsensusClusterConfiguration.codec.write(stream, (JointConsensusClusterConfiguration) value);
            } else {
                StableClusterConfiguration.codec.write(stream, (StableClusterConfiguration) value);
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
};