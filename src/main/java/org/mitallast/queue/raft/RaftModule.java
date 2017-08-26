package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.FilePersistentService;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.raft.resource.ResourceRegistry;

public class RaftModule extends AbstractModule {
    static {
        Codec.register(200, AddServer.class, AddServer.codec);
        Codec.register(201, AddServerResponse.class, AddServerResponse.codec);
        Codec.register(202, AppendEntries.class, AppendEntries.codec);
        Codec.register(203, AppendRejected.class, AppendRejected.codec);
        Codec.register(204, AppendSuccessful.class, AppendSuccessful.codec);
        Codec.register(205, ClientMessage.class, ClientMessage.codec);
        Codec.register(206, DeclineCandidate.class, DeclineCandidate.codec);
        Codec.register(207, InstallSnapshot.class, InstallSnapshot.codec);
        Codec.register(208, InstallSnapshotRejected.class, InstallSnapshotRejected.codec);
        Codec.register(209, InstallSnapshotSuccessful.class, InstallSnapshotSuccessful.codec);
        Codec.register(210, LogEntry.class, LogEntry.codec);
        Codec.register(211, Noop.class, Noop.codec);
        Codec.register(212, RaftSnapshot.class, RaftSnapshot.codec);
        Codec.register(213, RaftSnapshotMetadata.class, RaftSnapshotMetadata.codec);
        Codec.register(214, RemoveServer.class, RemoveServer.codec);
        Codec.register(215, RemoveServerResponse.class, RemoveServerResponse.codec);
        Codec.register(216, RequestVote.class, RequestVote.codec);
        Codec.register(217, VoteCandidate.class, VoteCandidate.codec);

        Codec.register(218, ClusterConfiguration.class, ClusterConfiguration.codec);
        Codec.register(219, JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration.codec);
        Codec.register(220, StableClusterConfiguration.class, StableClusterConfiguration.codec);
    }

    @Override
    protected void configure() {
        bind(Raft.class).asEagerSingleton();
        bind(RaftHandler.class).asEagerSingleton();
        bind(DefaultRaftContext.class).asEagerSingleton();
        bind(FilePersistentService.class).asEagerSingleton();

        bind(ClusterDiscovery.class).asEagerSingleton();
        bind(PersistentService.class).to(FilePersistentService.class);

        bind(RaftContext.class).to(DefaultRaftContext.class);

        bind(ResourceRegistry.class).asEagerSingleton();
    }
}
